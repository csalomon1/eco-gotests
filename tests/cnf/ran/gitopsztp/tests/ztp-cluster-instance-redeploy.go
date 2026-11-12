package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/argocd"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/bmh"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/hive"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/ocm"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"
	argocdtypes "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/argocd/argocdtypes/v1alpha1"
	siteconfigv1alpha1 "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/siteconfig/v1alpha1"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/siteconfig"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/gitopsztp/internal/tsparams"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/alerter"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/querier"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/rancluster"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/ranparam"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/version"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/raninittools"
)

// CNF-24973 verifies the lifecycle of a running bare-metal MNO spoke managed through ClusterInstance.
var _ = Describe("ZTP ClusterInstance redeploy", Label(tsparams.LabelClusterInstanceDeleteTestCases), func() {
	var (
		clustersApp            *argocd.ApplicationBuilder
		originalSyncPolicy     *argocdtypes.SyncPolicy
		autoSyncPaused         bool
		clusterInstanceDeleted bool
		probeRule              *monitoringv1.PrometheusRule
		cleanupQuerierAccess   bool
	)

	BeforeEach(func() {
		spokeVersion, err := version.GetOCPVersion(Spoke1APIClient)
		Expect(err).ToNot(HaveOccurred(), "Failed to get spoke OpenShift version")

		supportedVersion, err := version.IsVersionStringInRange(spokeVersion, "4.22.0-0", "")
		Expect(err).ToNot(HaveOccurred(), "Failed to check spoke OpenShift version")
		if !supportedVersion {
			Skip("CNF-24973 requires OpenShift 4.22 or later")
		}

		clustersApp, err = argocd.PullApplication(
			HubAPIClient, redeployClustersApplicationName(), ranparam.OpenshiftGitOpsNamespace)
		Expect(err).ToNot(HaveOccurred(), "Failed to get clusters Argo CD application")
		Expect(clustersApp.Object.Status.Sync.Status).To(Equal(argocdtypes.SyncStatusCodeSynced),
			"Clusters Argo CD application must be Synced before this test")
		Expect(string(clustersApp.Object.Status.Health.Status)).To(Equal("Healthy"),
			"Clusters Argo CD application must be Healthy before this test")

		if clustersApp.Definition.Spec.SyncPolicy != nil {
			originalSyncPolicy = clustersApp.Definition.Spec.SyncPolicy.DeepCopy()
		}
	})

	It("detaches and redeploys the running MNO spoke", reportxml.ID("24973"), func() {
		// Register recovery in dependency order. Ginkgo executes these cleanups in reverse order.
		DeferCleanup(func() {
			if cleanupQuerierAccess {
				Expect(querier.CleanupQuerierResources(HubAPIClient)).
					To(Succeed(), "Failed to remove temporary ACM metrics access")
			}
		})
		DeferCleanup(func() {
			if probeRule != nil {
				err := Spoke1APIClient.Delete(context.Background(), probeRule)
				Expect(err == nil || apierrors.IsNotFound(err)).To(BeTrue(),
					"Failed to delete temporary spoke alert rule: %v", err)
			}
		})
		DeferCleanup(func() {
			if autoSyncPaused {
				Expect(setClustersSyncPolicy(originalSyncPolicy)).To(Succeed(),
					"Failed to restore clusters app sync policy")
			}
		})
		DeferCleanup(func() {
			if clusterInstanceDeleted {
				Expect(syncClustersApplication(60*time.Minute)).To(Succeed(),
					"Failed to restore the deleted ClusterInstance")
			}
		})

		By("checking the spoke is a multi-node cluster")
		controlPlaneNodes, err := rancluster.ListNodesByLabel(Spoke1APIClient, RANConfig.ControlPlaneLabelMap)
		Expect(err).ToNot(HaveOccurred(), "Failed to list spoke control-plane nodes")
		if len(controlPlaneNodes) < 2 {
			Skip("CNF-24973 applies only to a multi-node spoke")
		}

		By("checking the spoke is operational and recording its version and node count")
		originalVersion, err := version.GetOCPVersion(Spoke1APIClient)
		Expect(err).ToNot(HaveOccurred(), "Spoke API is unavailable")
		originalNodeCount, err := readySpokeNodeCount()
		Expect(err).ToNot(HaveOccurred(), "Spoke nodes are not all Ready")
		Expect(originalNodeCount).To(BeNumerically(">", 0), "Spoke has no nodes")

		By("checking the ClusterInstance is provisioned on the hub")
		clusterInstance, err := siteconfig.PullClusterInstance(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
		Expect(err).ToNot(HaveOccurred(), "Failed to find spoke ClusterInstance")
		_, err = clusterInstance.WaitForCondition(metav1.Condition{
			Type: string(siteconfigv1alpha1.ClusterProvisioned), Reason: string(siteconfigv1alpha1.Completed),
		}, 5*time.Minute)
		Expect(err).ToNot(HaveOccurred(), "Spoke ClusterInstance is not provisioned")

		By("checking the spoke uses bare-metal hosts")
		hosts, err := bmh.List(HubAPIClient, RANConfig.Spoke1Name)
		Expect(err).ToNot(HaveOccurred(), "Failed to list spoke BareMetalHosts")
		Expect(hosts).ToNot(BeEmpty(), "CNF-24973 requires bare-metal hosts")

		clusterDeployment, err := hive.PullClusterDeployment(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
		Expect(err).ToNot(HaveOccurred(), "Failed to find spoke ClusterDeployment")
		originalClusterID := clusterDeployment.Object.Spec.ClusterMetadata.ClusterID
		originalInfraID := clusterDeployment.Object.Spec.ClusterMetadata.InfraID
		Expect(originalClusterID).ToNot(BeEmpty(), "ClusterDeployment has no cluster ID")
		Expect(originalInfraID).ToNot(BeEmpty(), "ClusterDeployment has no infra ID")
		managedCluster, err := ocm.PullManagedCluster(HubAPIClient, RANConfig.Spoke1Name)
		Expect(err).ToNot(HaveOccurred(), "Failed to find the spoke ManagedCluster")
		_, err = managedCluster.WaitForCondition(metav1.Condition{
			Type: "ManagedClusterConditionAvailable", Status: metav1.ConditionTrue,
		}, 5*time.Minute)
		Expect(err).ToNot(HaveOccurred(), "Spoke ManagedCluster is not Available before detachment")

		By("connecting to ACM Observability and recording the spoke metrics and alerts")
		cleanupQuerierAccess, err = querierResourcesNeedCleanup()
		Expect(err).ToNot(HaveOccurred(), "Failed to check ACM metrics access resources")
		metricsAPI, err := newACMMetricsClient()
		Expect(err).ToNot(HaveOccurred(), "Failed to create ACM metrics client")
		alertsAPI, err := alerter.CreateAlerterClientForCluster(HubAPIClient)
		Expect(err).ToNot(HaveOccurred(), "Failed to create ACM Alertmanager client")

		testID := uuid.NewString()
		probeRule, err = createRedeployProbeRule(testID)
		Expect(err).ToNot(HaveOccurred(), "Failed to create temporary spoke alert rule")

		Eventually(func() (bool, error) {
			return redeployACMSpokeMetricExists(metricsAPI, RANConfig.Spoke1Name)
		}).WithTimeout(12*time.Minute).WithPolling(30*time.Second).Should(BeTrue(),
			"ACM did not receive spoke metrics before detachment")
		Eventually(func() (bool, error) {
			return redeployACMSpokeAlertExists(alertsAPI, testID)
		}).WithTimeout(5*time.Minute).WithPolling(10*time.Second).Should(BeTrue(),
			"ACM did not receive the probe alert before detachment")

		By("pausing automated sync so Argo CD does not immediately recreate the deleted object")
		if clustersApp.Definition.Spec.SyncPolicy != nil &&
			clustersApp.Definition.Spec.SyncPolicy.Automated != nil {
			pausedPolicy := clustersApp.Definition.Spec.SyncPolicy.DeepCopy()
			pausedPolicy.Automated = nil
			autoSyncPaused = true
			Expect(setClustersSyncPolicy(pausedPolicy)).To(Succeed(), "Failed to pause clusters app auto-sync")
		}

		By("deleting the ClusterInstance to detach the spoke")
		clusterInstanceDeleted = true
		Expect(clusterInstance.Delete()).To(Succeed(), "Failed to delete spoke ClusterInstance")
		Expect(waitForClusterInstance(RANConfig.Spoke1Name, RANConfig.Spoke1Name, false, 30*time.Minute)).To(Succeed(),
			"ClusterInstance was not removed from the hub")
		Expect(waitForManagedCluster(RANConfig.Spoke1Name, false, 15*time.Minute)).To(Succeed(),
			"ManagedCluster was not detached from ACM")
		detachedAt := time.Now()

		By("checking the spoke remains operational while detached")
		detachedVersion, err := version.GetOCPVersion(Spoke1APIClient)
		Expect(err).ToNot(HaveOccurred(), "Spoke API is unavailable after detachment")
		Expect(detachedVersion).To(Equal(originalVersion), "Spoke version changed while detached")
		detachedNodeCount, err := readySpokeNodeCount()
		Expect(err).ToNot(HaveOccurred(), "Spoke nodes are not Ready while detached")
		Expect(detachedNodeCount).To(Equal(originalNodeCount), "Spoke node count changed while detached")

		By("checking ACM no longer receives spoke metrics or new alert updates")
		Eventually(func() (bool, error) {
			return redeployACMSpokeMetricExists(metricsAPI, RANConfig.Spoke1Name)
		}).WithTimeout(15*time.Minute).WithPolling(30*time.Second).Should(BeFalse(),
			"ACM continued to return spoke metrics after detachment")
		// Alertmanager can retain an alert after its source stops sending updates.
		Consistently(func() (bool, error) {
			return redeployACMSpokeAlertUpdatedAfter(alertsAPI, testID, detachedAt)
		}).WithDuration(10*time.Minute).WithPolling(15*time.Second).Should(BeFalse(),
			"ACM continued to receive spoke alert updates after detachment")

		By("recreating the ClusterInstance from the clusters Argo CD application")
		Expect(syncClustersApplication(60*time.Minute)).To(Succeed(),
			"Failed to redeploy the spoke from the clusters app")
		clusterInstanceDeleted = false
		redeployedAt := time.Now()

		By("checking the spoke reattached and hub management resumed")
		Eventually(func() (bool, error) {
			return redeployACMSpokeMetricExists(metricsAPI, RANConfig.Spoke1Name)
		}).WithTimeout(15*time.Minute).WithPolling(30*time.Second).Should(BeTrue(),
			"ACM did not resume receiving spoke metrics")
		Eventually(func() (bool, error) {
			return redeployACMSpokeAlertUpdatedAfter(alertsAPI, testID, redeployedAt)
		}).WithTimeout(10*time.Minute).WithPolling(15*time.Second).Should(BeTrue(),
			"ACM did not resume receiving spoke alert updates")

		redeployedVersion, err := version.GetOCPVersion(Spoke1APIClient)
		Expect(err).ToNot(HaveOccurred(), "Spoke API is unavailable after redeployment")
		Expect(redeployedVersion).To(Equal(originalVersion), "Spoke version changed after redeployment")
		redeployedNodeCount, err := readySpokeNodeCount()
		Expect(err).ToNot(HaveOccurred(), "Spoke nodes are not Ready after redeployment")
		Expect(redeployedNodeCount).To(Equal(originalNodeCount), "Spoke node count changed after redeployment")

		clusterDeployment, err = hive.PullClusterDeployment(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
		Expect(err).ToNot(HaveOccurred(), "Failed to find ClusterDeployment after redeployment")
		Expect(clusterDeployment.Object.Spec.ClusterMetadata.ClusterID).To(Equal(originalClusterID))
		Expect(clusterDeployment.Object.Spec.ClusterMetadata.InfraID).To(Equal(originalInfraID))

		By("restoring the clusters app automated sync policy")
		if autoSyncPaused {
			Expect(setClustersSyncPolicy(originalSyncPolicy)).To(Succeed(),
				"Failed to restore clusters app auto-sync")
			autoSyncPaused = false
		}
	})
})
