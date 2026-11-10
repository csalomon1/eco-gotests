package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	alertmanagerv2 "github.com/prometheus/alertmanager/api/v2/client"
	alertmanageralert "github.com/prometheus/alertmanager/api/v2/client/alert"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/argocd"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/assisted"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/bmh"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/configmap"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/hive"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/namespace"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/ocm"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/pod"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/reportxml"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/route"
	argocdtypes "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/argocd/argocdtypes/v1alpha1"
	ocmclusterv1 "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/ocm/clusterv1"
	siteconfigv1alpha1 "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/siteconfig/v1alpha1"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/siteconfig"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/gitopsztp/internal/gitdetails"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/gitopsztp/internal/tsparams"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/alerter"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/querier"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/rancluster"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/ranhelper"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/ranparam"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/version"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"

	. "github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/raninittools"
)

var _ = Describe("ZTP Siteconfig Operator's Cluster Instance Delete Tests",
	Label(tsparams.LabelClusterInstanceDeleteTestCases), func() {
		var (
			clustersApp                *argocd.ApplicationBuilder
			originalClustersGitPath    string
			originalClustersSyncPolicy *argocdtypes.SyncPolicy
			clusterInstanceForCleanup  *siteconfigv1alpha1.ClusterInstance
			clusterInstanceDeleted     bool
			clusterRedeployProbeRule   *monitoringv1.PrometheusRule
			acmObservabilityMetricsAPI v1.API
			acmObservabilityAlertsAPI  *alertmanagerv2.AlertmanagerAPI
			querierResourcesCreated    bool
		)

		BeforeEach(func() {
			By("verifying that ZTP meets the minimum version")

			versionInRange, err := version.IsVersionStringInRange(RANConfig.ZTPVersion, "4.17.0-0", "")
			Expect(err).ToNot(HaveOccurred(), "Failed to compare ZTP version string")

			if !versionInRange {
				Skip("ZTP Siteconfig operator tests require ZTP 4.17 or later")
			}

			By("saving the original clusters app source")

			clustersApp, err = argocd.PullApplication(
				HubAPIClient, tsparams.ArgoCdClustersAppName, ranparam.OpenshiftGitOpsNamespace)
			Expect(err).ToNot(HaveOccurred(), "Failed to get the original clusters app")

			originalClustersGitPath, err = gitdetails.GetGitPath(clustersApp)
			Expect(err).ToNot(HaveOccurred(), "Failed to get the original clusters app git path")
			originalClustersSyncPolicy = clustersApp.Definition.Spec.SyncPolicy.DeepCopy()
		})

		AfterEach(func() {
			if CurrentSpecReport().State.Is(types.SpecStateSkipped) {
				return
			}

			if clusterRedeployProbeRule != nil {
				By("removing the temporary spoke alert rule")

				err := Spoke1APIClient.Delete(context.Background(), clusterRedeployProbeRule)
				if err != nil && !k8serrors.IsNotFound(err) {
					Expect(err).ToNot(HaveOccurred(), "Failed to remove the temporary spoke alert rule")
				}
				clusterRedeployProbeRule = nil
			}

			By("resetting the clusters app back to the original settings")

			clustersApp.Definition.Spec.Source.Path = originalClustersGitPath
			clustersApp.Definition.Spec.SyncPolicy = originalClustersSyncPolicy.DeepCopy()
			updatedApp, err := clustersApp.Update(true)
			Expect(err).ToNot(HaveOccurred(), "Failed to update clusters app back to the original settings")

			if clusterInstanceDeleted && clusterInstanceForCleanup != nil {
				By("restoring the deleted cluster instance after the test")

				err := recreateClusterInstance(clusterInstanceForCleanup, 60*time.Minute)
				Expect(err).ToNot(HaveOccurred(), "Failed to restore the deleted spoke cluster instance")
				clusterInstanceDeleted = false
			}
			clusterInstanceForCleanup = nil

			if querierResourcesCreated {
				By("removing temporary hub metrics query permissions")

				err := querier.CleanupQuerierResources(HubAPIClient)
				Expect(err).ToNot(HaveOccurred(), "Failed to remove temporary hub metrics query permissions")
				querierResourcesCreated = false
			}

			By("waiting for the clusters app to sync")

			err = updatedApp.WaitForSourceUpdate(true, tsparams.ArgoCdChangeTimeout)
			Expect(err).ToNot(HaveOccurred(), "Failed to wait for clusters app to sync")

			By("checking the infra env manifests exists on hub")

			infraEnvList, err := assisted.ListInfraEnvs(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke infra env manifests")
			Expect(infraEnvList).ToNot(BeEmpty(), "Failed to find spoke infra env manifests")

			By("checking the bare metal host manifests exists on hub")

			bmhList, err := bmh.List(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke bmh manifests")
			Expect(bmhList).ToNot(BeEmpty(), "Failed to find spoke bmh manifests")

			By("checking the cluster deployment manifests exists on hub")

			_, err = hive.PullClusterDeployment(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke cluster deployment manifests")

			By("checking the NM state config manifests exists on hub")

			nmStateConfigList, err := assisted.ListNmStateConfigs(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list NM state config manifests")
			Expect(nmStateConfigList).ToNot(BeEmpty(), "Failed to find NM state config manifests")

			By("checking the klusterlet addon config manifests exists on hub")

			_, err = ocm.PullKAC(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke kac manifests")

			By("checking the agent cluster install manifests exists on hub")

			_, err = assisted.PullAgentClusterInstall(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke agent cluster install manifests")
		})

		// 75374 - Detaching the AI multi-node openshift (MNO) spoke cluster.
		It("Validate detaching the AI multi-node openshift spoke cluster", reportxml.ID("75374"), func() {
			By("checking spoke cluster type")

			spokeClusterType, err := rancluster.CheckSpokeClusterType(RANConfig.Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to fetch spoke cluster type")

			if spokeClusterType == ranparam.SNOCluster {
				Skip("This test only applies to standard or multi-node openshift spoke cluster")
			}

			// The clusters app is updated later on, but skip early if the git path does not exist to avoid
			// extra cleanup.
			By("checking if the ztp test path exists")

			if !clustersApp.DoesGitPathExist(tsparams.ZtpTestPathDetachAIMNO) {
				Skip(fmt.Sprintf("git path '%s' could not be found", tsparams.ZtpTestPathDetachAIMNO))
			}

			By("deleting default assisted installer template reference ConfigMap custom resources")

			By("deleting default assisted installer cluster level templates ConfigMap CR")

			clusterTemplateConfigMap, err := configmap.Pull(HubAPIClient, tsparams.DefaultAIClusterTemplatesConfigMapName,
				RANConfig.AcmOperatorNamespace)
			if err == nil {
				err = clusterTemplateConfigMap.Delete()
				Expect(err).ToNot(HaveOccurred(), "Failed to delete AI cluster level templates config map")
			}

			By("deleting default assisted installer node level templates ConfigMap CR")

			nodeTemplateConfigMap, err := configmap.Pull(HubAPIClient, tsparams.DefaultAINodeTemplatesConfigMapName,
				RANConfig.AcmOperatorNamespace)
			if err == nil {
				err = nodeTemplateConfigMap.Delete()
				Expect(err).ToNot(HaveOccurred(), "Failed to delete AI node level templates config map")
			}

			By("verifying installed spoke cluster should still be functional")

			_, err = version.GetOCPVersion(Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to get OCP version from spoke and verify spoke cluster access")

			By("updating the clusters app git path")

			err = gitdetails.UpdateAndWaitForSync(clustersApp, true, tsparams.ZtpTestPathDetachAIMNO)
			Expect(err).ToNot(HaveOccurred(), "Failed to update the clusters app git path")

			validateAISpokeClusterInstallCRsRemoved()

			By("deleting siteconfig operator pod running under rhacm namespace on hub cluster")

			By("Get the siteconfig operator pod name with label " + tsparams.SiteconfigOperatorPodLabel)
			desiredPodName, err := ranhelper.GetPodNameWithLabel(HubAPIClient, RANConfig.AcmOperatorNamespace,
				tsparams.SiteconfigOperatorPodLabel)
			Expect(err).ToNot(HaveOccurred(), "Failed to get siteconfig operator pod name with label "+
				tsparams.SiteconfigOperatorPodLabel+" from "+RANConfig.AcmOperatorNamespace+" namespace")

			By("deleting the siteconfig operator pod name from namespace " + RANConfig.AcmOperatorNamespace)

			siteconfigOperatorPodName, err := pod.Pull(HubAPIClient, desiredPodName, RANConfig.AcmOperatorNamespace)
			if err == nil {
				_, err = siteconfigOperatorPodName.DeleteAndWait(3 * time.Minute)
				Expect(err).ToNot(HaveOccurred(), "Failed to delete siteconfig operator pod")
			}

			// Teardown test expected results validation.
			// The default assisted installer template reference ConfigMap custom resource should be recreated successfully.
			By("checking the default assisted installer template reference ConfigMap CRs recreated successfully")
			// Wait for 10 seconds to allow siteconfig operator to reconcile state after restarting controller pod.
			// before checking cluster-level and node-level templates configmap CR recreated on rhacm namespace.
			time.Sleep(10 * time.Second)

			By("checking default assisted installer cluster level templates ConfigMap CR exists")

			_, err = configmap.Pull(HubAPIClient, tsparams.DefaultAIClusterTemplatesConfigMapName,
				RANConfig.AcmOperatorNamespace)
			Expect(err).ToNot(HaveOccurred(), "Failed to find default AI cluster level templates config map")

			By("checking default assisted installer node level templates ConfigMap CR exists")

			_, err = configmap.Pull(HubAPIClient, tsparams.DefaultAINodeTemplatesConfigMapName,
				RANConfig.AcmOperatorNamespace)
			Expect(err).ToNot(HaveOccurred(), "Failed to find default AI node level templates config map")

			By("verifying installed spoke cluster should still be functional")

			_, err = version.GetOCPVersion(Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to get OCP version from spoke and verify spoke cluster access")

			By("verifying spoke cluster namespace CR exists on hub after siteconfig operator's pod restart")

			_, err = namespace.Pull(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke cluster namespace CR")

			By("verifying cluster instance CR exists on hub after siteconfig operator's pod restart")

			_, err = siteconfig.PullClusterInstance(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find cluster instance custom resource")
		})

		// 24973 - Detach and redeploy the AI multi-node openshift (MNO) spoke cluster.
		It("Validate detaching and redeploying the AI multi-node openshift spoke cluster", reportxml.ID("24973"), func() {
			By("checking spoke cluster type")

			spokeClusterType, err := rancluster.CheckSpokeClusterType(RANConfig.Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to fetch spoke cluster type")

			if spokeClusterType == ranparam.SNOCluster {
				Skip("This test only applies to standard or multi-node openshift spoke cluster")
			}

			By("checking that the spoke cluster instance is provisioned")

			clusterInstance, err := siteconfig.PullClusterInstance(
				HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke cluster instance before detachment")
			_, err = clusterInstance.WaitForCondition(metav1.Condition{
				Type:   string(siteconfigv1alpha1.ClusterProvisioned),
				Reason: string(siteconfigv1alpha1.Completed),
			}, 5*time.Minute)
			Expect(err).ToNot(HaveOccurred(), "Spoke cluster instance is not provisioned")
			clusterInstanceForCleanup = clusterInstance.Definition.DeepCopy()

			By("verifying the spoke uses bare metal hosts")

			spokeBmhList, err := bmh.List(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke bare metal hosts before detachment")
			Expect(spokeBmhList).ToNot(BeEmpty(), "Spoke has no bare metal hosts")

			By("recording the spoke cluster identity before detachment")

			clusterDeployment, err := hive.PullClusterDeployment(
				HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke cluster deployment before detachment")

			originalClusterID := clusterDeployment.Object.Spec.ClusterMetadata.ClusterID
			originalInfraID := clusterDeployment.Object.Spec.ClusterMetadata.InfraID
			Expect(originalClusterID).ToNot(BeEmpty(), "Spoke cluster deployment has no cluster ID")
			Expect(originalInfraID).ToNot(BeEmpty(), "Spoke cluster deployment has no infra ID")

			By("connecting to ACM Observability metrics")

			proxyRoute, err := route.Pull(
				HubAPIClient, "rbac-query-proxy", ranparam.ACMObservabilityNamespace)
			Expect(err).ToNot(HaveOccurred(), "Failed to find ACM Observability metrics route")
			Expect(proxyRoute.Definition.Status.Ingress).ToNot(BeEmpty(), "ACM Observability metrics route has no host")

			token, err := querier.GetQuerierToken(HubAPIClient)
			querierResourcesCreated = true
			Expect(err).ToNot(HaveOccurred(), "Failed to get a token for ACM Observability metrics")

			caPool, err := rancluster.GetClusterDefaultRouterCAPool(HubAPIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to get the hub router CA bundle")

			acmObservabilityMetricsAPI, err = querier.CreatePrometheusAPI(
				proxyRoute.Definition.Status.Ingress[0].Host, token, caPool)
			Expect(err).ToNot(HaveOccurred(), "Failed to create the ACM Observability metrics client")

			By("verifying that ACM currently receives metrics from the spoke")

			Eventually(func() (bool, error) {
				return acmSpokeMetricExists(acmObservabilityMetricsAPI, RANConfig.Spoke1Name)
			}).WithTimeout(12*time.Minute).WithPolling(30*time.Second).Should(BeTrue(),
				"ACM Observability did not return cluster_infrastructure_provider metrics for the spoke")

			By("connecting to the ACM Observability Alertmanager")

			acmObservabilityAlertsAPI, err = alerter.CreateAlerterClientForCluster(HubAPIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to create the ACM Observability Alertmanager client")

			testID := uuid.NewString()

			By("creating a temporary alert rule on the spoke to verify alert forwarding")

			clusterRedeployProbeRule, err = createClusterRedeployProbeRule(testID)
			Expect(err).ToNot(HaveOccurred(), "Failed to create a temporary spoke PrometheusRule")

			By("verifying that ACM currently receives the spoke alert")

			Eventually(func() (bool, error) {
				return acmSpokeAlertExists(acmObservabilityAlertsAPI, testID)
			}).WithTimeout(5*time.Minute).WithPolling(10*time.Second).Should(BeTrue(),
				"ACM Observability did not receive the temporary alert from the spoke")

			By("pausing Argo CD automated sync so it does not recreate the deleted ClusterInstance")

			if clustersApp.Definition.Spec.SyncPolicy == nil {
				clustersApp.Definition.Spec.SyncPolicy = &argocdtypes.SyncPolicy{}
			}
			clustersApp.Definition.Spec.SyncPolicy.Automated = nil
			updatedClustersApp, err := clustersApp.Update(false)
			Expect(err).ToNot(HaveOccurred(), "Failed to pause Argo CD automated sync")
			clustersApp = updatedClustersApp

			By("deleting the spoke ClusterInstance to detach it from the hub")

			err = clusterInstance.Delete()
			Expect(err).ToNot(HaveOccurred(), "Failed to delete the spoke ClusterInstance")
			clusterInstanceDeleted = true

			By("waiting for the ClusterInstance to be removed from the hub")

			err = waitForClusterInstanceAbsent(RANConfig.Spoke1Name, 30*time.Minute)
			Expect(err).ToNot(HaveOccurred(), "ClusterInstance was not removed from the hub")

			By("waiting for the managed cluster to be detached from the hub")

			err = waitForManagedClusterAbsent(RANConfig.Spoke1Name, 15*time.Minute)
			Expect(err).ToNot(HaveOccurred(), "ManagedCluster was not detached from the hub")

			By("verifying the spoke remains operational while detached")

			_, err = version.GetOCPVersion(Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Spoke cluster is not operational after detachment")
			verifySpokeNodesReady()

			By("verifying ACM stops receiving spoke metrics while detached")

			Eventually(func() (bool, error) {
				return acmSpokeMetricExists(acmObservabilityMetricsAPI, RANConfig.Spoke1Name)
			}).WithTimeout(15*time.Minute).WithPolling(30*time.Second).Should(BeFalse(),
				"ACM Observability continued returning spoke metrics after detachment")

			By("verifying ACM stops receiving spoke alerts while detached")

			Eventually(func() (bool, error) {
				return acmSpokeAlertExists(acmObservabilityAlertsAPI, testID)
			}).WithTimeout(15*time.Minute).WithPolling(15*time.Second).Should(BeFalse(),
				"ACM Alertmanager continued returning the spoke alert after detachment")

			By("recreating the saved ClusterInstance to reattach the running spoke")

			err = recreateClusterInstance(clusterInstanceForCleanup, 60*time.Minute)
			Expect(err).ToNot(HaveOccurred(), "Failed to recreate and provision the spoke ClusterInstance")
			clusterInstanceDeleted = false

			By("verifying ACM resumes receiving spoke metrics after reattachment")

			Eventually(func() (bool, error) {
				return acmSpokeMetricExists(acmObservabilityMetricsAPI, RANConfig.Spoke1Name)
			}).WithTimeout(15*time.Minute).WithPolling(30*time.Second).Should(BeTrue(),
				"ACM Observability did not resume receiving metrics from the spoke")

			By("verifying ACM resumes receiving spoke alerts after reattachment")

			Eventually(func() (bool, error) {
				return acmSpokeAlertExists(acmObservabilityAlertsAPI, testID)
			}).WithTimeout(15*time.Minute).WithPolling(15*time.Second).Should(BeTrue(),
				"ACM Alertmanager did not resume receiving alerts from the spoke")

			By("verifying the spoke remains operational after reattachment")

			_, err = version.GetOCPVersion(Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to access the spoke after reattachment")
			verifySpokeNodesReady()

			By("verifying redeployment preserved the spoke cluster identity")

			clusterDeployment, err = hive.PullClusterDeployment(
				HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke cluster deployment after redeployment")
			Expect(clusterDeployment.Object.Spec.ClusterMetadata.ClusterID).To(Equal(originalClusterID),
				"Redeployed spoke cluster has a different cluster ID")
			Expect(clusterDeployment.Object.Spec.ClusterMetadata.InfraID).To(Equal(originalInfraID),
				"Redeployed spoke cluster has a different infra ID")

			By("verifying the spoke cluster is available on the hub after reattachment")

			managedCluster, err := ocm.PullManagedCluster(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke managed cluster after redeployment")

			_, err = managedCluster.WaitForCondition(metav1.Condition{
				Type:   "ManagedClusterConditionAvailable",
				Status: metav1.ConditionTrue,
			}, 12*time.Minute)
			Expect(err).ToNot(HaveOccurred(), "Failed to wait for spoke managed cluster to become available")

			managedClusterID, err := rancluster.GetManagedClusterID(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to get spoke managed cluster ID after redeployment")
			Expect(managedClusterID).To(Equal(originalClusterID),
				"Redeployed spoke managed cluster has a different cluster ID")

			By("verifying hub management resources are restored")

			infraEnvList, err := assisted.ListInfraEnvs(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke infra envs after redeployment")
			Expect(infraEnvList).ToNot(BeEmpty(), "Failed to find spoke infra env after redeployment")

			bmhList, err := bmh.List(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke bare metal hosts after redeployment")
			Expect(bmhList).ToNot(BeEmpty(), "Failed to find spoke bare metal host after redeployment")

			nmStateConfigList, err := assisted.ListNmStateConfigs(HubAPIClient, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to list spoke NM state configs after redeployment")
			Expect(nmStateConfigList).ToNot(BeEmpty(), "Spoke NM state configs are missing after redeployment")

			_, err = assisted.PullAgentClusterInstall(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke agent cluster install after redeployment")

			_, err = ocm.PullKAC(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
			Expect(err).ToNot(HaveOccurred(), "Failed to find spoke klusterlet addon config after redeployment")

			By("removing the temporary spoke alert rule")

			err = Spoke1APIClient.Delete(context.Background(), clusterRedeployProbeRule)
			Expect(err).ToNot(HaveOccurred(), "Failed to remove the temporary spoke alert rule")
			clusterRedeployProbeRule = nil
		})

		// 75376 - Detaching the AI single-node openshift (SNO) spoke cluster.
		It("Validate detaching the AI single-node openshift spoke cluster", reportxml.ID("75376"), func() {
			By("checking spoke cluster type")

			spokeClusterType, err := rancluster.CheckSpokeClusterType(RANConfig.Spoke1APIClient)
			Expect(err).ToNot(HaveOccurred(), "Failed to fetch spoke cluster type")

			if spokeClusterType == ranparam.HighlyAvailableCluster {
				Skip("This test only applies to single-node openshift spoke cluster")
			}

			By("checking if the ztp test path exists")

			if !clustersApp.DoesGitPathExist(tsparams.ZtpTestPathDetachAISNO) {
				Skip(fmt.Sprintf("git path '%s' could not be found", tsparams.ZtpTestPathDetachAISNO))
			}

			By("updating the clusters app git path")

			err = gitdetails.UpdateAndWaitForSync(clustersApp, true, tsparams.ZtpTestPathDetachAISNO)
			Expect(err).ToNot(HaveOccurred(), "Failed to update the clusters app git path")

			validateAISpokeClusterInstallCRsRemoved()
		})
	})

// validateAISpokeClusterInstallCRsRemoved verifies AI spoke cluster install CRs removed and spoke cluster accessible.
//
//nolint:wsl
func validateAISpokeClusterInstallCRsRemoved() {
	By("checking the infra env manifests removed on hub")

	infraEnvList, err := assisted.ListInfraEnvs(HubAPIClient, RANConfig.Spoke1Name)
	Expect(err).ToNot(HaveOccurred(), "Failed to list spoke infra env manifests")
	Expect(infraEnvList).To(BeEmpty(), "Found spoke infra env manifests but expected to be removed")

	By("checking the bare metal host manifests removed on hub")

	bmhList, err := bmh.List(HubAPIClient, RANConfig.Spoke1Name)
	Expect(err).ToNot(HaveOccurred(), "Failed to list spoke bmh manifests")
	Expect(bmhList).To(BeEmpty(), "Found spoke bmh manifests but expected to be removed")

	By("checking the cluster deployment manifests removed on hub")

	_, err = hive.PullClusterDeployment(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
	Expect(err).To(HaveOccurred(), "Found spoke cluster deployment manifests but expected to be removed")

	By("checking the NM state config manifests removed on hub")

	nmStateConfigList, err := assisted.ListNmStateConfigs(HubAPIClient, RANConfig.Spoke1Name)
	Expect(err).ToNot(HaveOccurred(), "Failed to list NM state config manifests")
	Expect(nmStateConfigList).To(BeEmpty(), "Found spoke NM state config manifests but expected to be removed")

	By("checking the klusterlet addon config manifests removed on hub")

	_, err = ocm.PullKAC(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
	Expect(err).To(HaveOccurred(), "Found spoke kac manifests but expected to be removed")

	By("checking the agent cluster install manifests removed on hub")

	_, err = assisted.PullAgentClusterInstall(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
	Expect(err).To(HaveOccurred(), "Found spoke ACI manifests but expected to be removed")

	By("verifying installed spoke cluster should still be functional")

	_, err = version.GetOCPVersion(Spoke1APIClient)
	Expect(err).ToNot(HaveOccurred(), "Failed to get OCP version from spoke and verify spoke cluster access")
}

func createClusterRedeployProbeRule(testID string) (*monitoringv1.PrometheusRule, error) {
	if err := Spoke1APIClient.AttachScheme(monitoringv1.AddToScheme); err != nil {
		return nil, fmt.Errorf("failed to register PrometheusRule API scheme: %w", err)
	}

	rule := &monitoringv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eco-gotests-redeploy-probe-" + uuid.NewString(),
			Namespace: ranparam.OpenshiftMonitoringNamespace,
			Labels: map[string]string{
				"prometheus": "k8s",
				"role":       "alert-rules",
			},
		},
		Spec: monitoringv1.PrometheusRuleSpec{
			Groups: []monitoringv1.RuleGroup{{
				Name: "eco-gotests-cluster-redeploy",
				Rules: []monitoringv1.Rule{{
					Alert: "EcoGotestsClusterRedeployProbe",
					Expr:  intstr.FromString("vector(1)"),
					Labels: map[string]string{
						"severity":            "warning",
						"eco_gotests_test_id": testID,
						"managed_cluster":     RANConfig.Spoke1Name,
					},
					Annotations: map[string]string{
						"summary": "Temporary alert used by the eco-gotests cluster redeploy test",
					},
				}},
			}},
		},
	}

	if err := Spoke1APIClient.Create(context.Background(), rule); err != nil {
		return nil, fmt.Errorf("failed to create PrometheusRule %s: %w", rule.Name, err)
	}

	return rule, nil
}

func acmSpokeMetricExists(api v1.API, spokeName string) (bool, error) {
	query := fmt.Sprintf(`cluster_infrastructure_provider{cluster=%q}`, spokeName)
	result, _, err := api.Query(context.Background(), query, time.Now())
	if err != nil {
		return false, fmt.Errorf("failed to query ACM Observability for spoke metrics: %w", err)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return false, fmt.Errorf("unexpected ACM Observability query result %T", result)
	}

	return len(vector) > 0, nil
}

func verifySpokeNodesReady() {
	nodeList, err := Spoke1APIClient.K8sClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	Expect(err).ToNot(HaveOccurred(), "Failed to list spoke nodes")
	Expect(nodeList.Items).ToNot(BeEmpty(), "Spoke returned no nodes")

	for _, node := range nodeList.Items {
		nodeReady := false
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				nodeReady = true
				break
			}
		}

		Expect(nodeReady).To(BeTrue(), "Spoke node %s is not Ready", node.Name)
	}
}

func acmSpokeAlertExists(api *alertmanagerv2.AlertmanagerAPI, testID string) (bool, error) {
	filter := fmt.Sprintf(`eco_gotests_test_id=%q`, testID)
	response, err := api.Alert.GetAlerts(
		alertmanageralert.NewGetAlertsParamsWithTimeout(30 * time.Second).WithFilter([]string{filter}))
	if err != nil {
		return false, fmt.Errorf("failed to query ACM Alertmanager: %w", err)
	}

	return len(response.Payload) > 0, nil
}

func waitForClusterInstanceAbsent(name string, timeout time.Duration) error {
	if err := HubAPIClient.AttachScheme(siteconfigv1alpha1.AddToScheme); err != nil {
		return fmt.Errorf("failed to register ClusterInstance scheme: %w", err)
	}

	return wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			instance := &siteconfigv1alpha1.ClusterInstance{}
			err := HubAPIClient.Get(ctx, k8stypes.NamespacedName{Name: name, Namespace: name}, instance)
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}

			return false, nil
		})
}

func waitForManagedClusterAbsent(name string, timeout time.Duration) error {
	if err := HubAPIClient.AttachScheme(ocmclusterv1.Install); err != nil {
		return fmt.Errorf("failed to register ManagedCluster scheme: %w", err)
	}

	return wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			managedCluster := &ocmclusterv1.ManagedCluster{}
			err := HubAPIClient.Get(ctx, k8stypes.NamespacedName{Name: name}, managedCluster)
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}

			return false, nil
		})
}

func recreateClusterInstance(original *siteconfigv1alpha1.ClusterInstance, timeout time.Duration) error {
	if err := HubAPIClient.AttachScheme(siteconfigv1alpha1.AddToScheme); err != nil {
		return fmt.Errorf("failed to register ClusterInstance scheme: %w", err)
	}

	current := &siteconfigv1alpha1.ClusterInstance{}
	err := HubAPIClient.Get(context.Background(), k8stypes.NamespacedName{
		Name: original.Name, Namespace: original.Namespace,
	}, current)
	if err == nil && current.DeletionTimestamp == nil {
		builder, err := siteconfig.PullClusterInstance(HubAPIClient, original.Name, original.Namespace)
		if err != nil {
			return fmt.Errorf("failed to pull existing ClusterInstance: %w", err)
		}

		_, err = builder.WaitForCondition(metav1.Condition{
			Type:   string(siteconfigv1alpha1.ClusterProvisioned),
			Reason: string(siteconfigv1alpha1.Completed),
		}, timeout)
		return err
	}
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to check current cluster instance: %w", err)
	}

	if err = waitForClusterInstanceAbsent(original.Name, timeout); err != nil {
		return fmt.Errorf("waiting for the old cluster instance to be removed: %w", err)
	}

	restored := original.DeepCopy()
	restored.ResourceVersion = ""
	restored.UID = ""
	restored.Generation = 0
	restored.CreationTimestamp = metav1.Time{}
	restored.DeletionTimestamp = nil
	restored.DeletionGracePeriodSeconds = nil
	restored.ManagedFields = nil
	restored.OwnerReferences = nil
	restored.Finalizers = nil
	restored.Status = siteconfigv1alpha1.ClusterInstanceStatus{}

	builder := siteconfig.NewCIBuilder(HubAPIClient, restored.Name, restored.Namespace)
	if builder == nil {
		return fmt.Errorf("failed to create a ClusterInstance builder")
	}
	builder.Definition = restored
	if _, err = builder.Create(); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to recreate ClusterInstance %s/%s: %w", restored.Namespace, restored.Name, err)
		}

		builder, err = siteconfig.PullClusterInstance(HubAPIClient, restored.Name, restored.Namespace)
		if err != nil {
			return fmt.Errorf("ClusterInstance appeared during recreation but could not be fetched: %w", err)
		}
	}

	_, err = builder.WaitForCondition(metav1.Condition{
		Type:   string(siteconfigv1alpha1.ClusterProvisioned),
		Reason: string(siteconfigv1alpha1.Completed),
	}, timeout)
	if err != nil {
		return fmt.Errorf("waiting for recreated ClusterInstance provisioning to complete: %w", err)
	}

	return nil
}
