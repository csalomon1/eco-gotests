package tests

import (
	"context"
	"fmt"
	"os"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	alertmanagerv2 "github.com/prometheus/alertmanager/api/v2/client"
	alertmanageralert "github.com/prometheus/alertmanager/api/v2/client/alert"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/argocd"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/nodes"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/ocm"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/route"
	argocdtypes "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/argocd/argocdtypes/v1alpha1"
	ocmclusterv1 "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/ocm/clusterv1"
	siteconfigv1alpha1 "github.com/rh-ecosystem-edge/eco-goinfra/pkg/schemes/siteconfig/v1alpha1"
	"github.com/rh-ecosystem-edge/eco-goinfra/pkg/siteconfig"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/gitopsztp/internal/tsparams"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/querier"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/rancluster"
	"github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/ranparam"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	goclient "sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/rh-ecosystem-edge/eco-gotests/tests/cnf/ran/internal/raninittools"
)

func redeployClustersApplicationName() string {
	if name := os.Getenv("ECO_CNF_RAN_GITOPS_CLUSTERS_APP_NAME"); name != "" {
		return name
	}

	return tsparams.ArgoCdClustersAppName
}

func newACMMetricsClient() (prometheusv1.API, error) {
	metricsRoute, err := route.Pull(HubAPIClient, "rbac-query-proxy", ranparam.ACMObservabilityNamespace)
	if err != nil {
		return nil, err
	}
	if len(metricsRoute.Definition.Status.Ingress) == 0 {
		return nil, fmt.Errorf("ACM metrics route has no ingress")
	}

	token, err := querier.GetQuerierToken(HubAPIClient)
	if err != nil {
		return nil, err
	}
	caPool, err := rancluster.GetClusterDefaultRouterCAPool(HubAPIClient)
	if err != nil {
		return nil, err
	}

	return querier.CreatePrometheusAPI(metricsRoute.Definition.Status.Ingress[0].Host, token, caPool)
}

func createRedeployProbeRule(testID string) (*monitoringv1.PrometheusRule, error) {
	if err := Spoke1APIClient.AttachScheme(monitoringv1.AddToScheme); err != nil {
		return nil, err
	}

	rule := &monitoringv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eco-gotests-redeploy-" + testID,
			Namespace: ranparam.OpenshiftMonitoringNamespace,
			Labels:    map[string]string{"prometheus": "k8s", "role": "alert-rules"},
		},
		Spec: monitoringv1.PrometheusRuleSpec{Groups: []monitoringv1.RuleGroup{{
			Name: "eco-gotests-cluster-redeploy",
			Rules: []monitoringv1.Rule{{
				Alert: "EcoGotestsClusterRedeployProbe",
				Expr:  intstr.FromString("vector(1)"),
				Labels: map[string]string{
					"severity": "warning", "eco_gotests_test_id": testID, "managed_cluster": RANConfig.Spoke1Name,
				},
				Annotations: map[string]string{"summary": "Temporary alert for CNF-24973"},
			}},
		}}},
	}
	if err := Spoke1APIClient.Create(context.Background(), rule); err != nil {
		return nil, err
	}

	return rule, nil
}

func readySpokeNodeCount() (int, error) {
	nodeList, err := nodes.List(Spoke1APIClient)
	if err != nil {
		return 0, err
	}
	if len(nodeList) == 0 {
		return 0, fmt.Errorf("spoke has no nodes")
	}

	for _, node := range nodeList {
		ready := false
		for _, condition := range node.Definition.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			return 0, fmt.Errorf("spoke node %s is not Ready", node.Definition.Name)
		}
	}

	return len(nodeList), nil
}

func waitForClusterInstance(name string, namespace string, present bool, timeout time.Duration) error {
	if err := HubAPIClient.AttachScheme(siteconfigv1alpha1.AddToScheme); err != nil {
		return err
	}

	return waitForHubObject(
		&siteconfigv1alpha1.ClusterInstance{},
		types.NamespacedName{Name: name, Namespace: namespace}, present, timeout)
}

func waitForManagedCluster(name string, present bool, timeout time.Duration) error {
	if err := HubAPIClient.AttachScheme(ocmclusterv1.Install); err != nil {
		return err
	}

	return waitForHubObject(
		&ocmclusterv1.ManagedCluster{},
		types.NamespacedName{Name: name}, present, timeout)
}

func waitForHubObject(object goclient.Object, key types.NamespacedName, present bool, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			err := HubAPIClient.Get(ctx, key, object)
			if apierrors.IsNotFound(err) {
				return !present, nil
			}
			if err != nil {
				return false, err
			}

			return present, nil
		})
}

func syncClustersApplication(timeout time.Duration) error {
	app, err := argocd.PullApplication(
		HubAPIClient, redeployClustersApplicationName(), ranparam.OpenshiftGitOpsNamespace)
	if err != nil {
		return err
	}

	app.Definition.Operation = &argocdtypes.Operation{
		Sync: &argocdtypes.SyncOperation{Prune: false},
	}
	if _, err = app.Update(false); err != nil {
		return err
	}
	if err = app.WaitForSourceUpdate(true, timeout); err != nil {
		return err
	}
	if err = waitForClusterInstance(RANConfig.Spoke1Name, RANConfig.Spoke1Name, true, timeout); err != nil {
		return err
	}

	clusterInstance, err := siteconfig.PullClusterInstance(HubAPIClient, RANConfig.Spoke1Name, RANConfig.Spoke1Name)
	if err != nil {
		return err
	}
	_, err = clusterInstance.WaitForCondition(metav1.Condition{
		Type: string(siteconfigv1alpha1.ClusterProvisioned), Reason: string(siteconfigv1alpha1.Completed),
	}, timeout)
	if err != nil {
		return err
	}
	if err = waitForManagedCluster(RANConfig.Spoke1Name, true, timeout); err != nil {
		return err
	}

	managedCluster, err := ocm.PullManagedCluster(HubAPIClient, RANConfig.Spoke1Name)
	if err != nil {
		return err
	}
	_, err = managedCluster.WaitForCondition(metav1.Condition{
		Type: "ManagedClusterConditionAvailable", Status: metav1.ConditionTrue,
	}, timeout)

	return err
}

func setClustersSyncPolicy(policy *argocdtypes.SyncPolicy) error {
	app, err := argocd.PullApplication(
		HubAPIClient, redeployClustersApplicationName(), ranparam.OpenshiftGitOpsNamespace)
	if err != nil {
		return err
	}

	if policy == nil {
		app.Definition.Spec.SyncPolicy = nil
	} else {
		app.Definition.Spec.SyncPolicy = policy.DeepCopy()
	}
	if _, err = app.Update(false); err != nil {
		return err
	}

	return app.WaitForSourceUpdate(true, 10*time.Minute)
}

func querierResourcesNeedCleanup() (bool, error) {
	if err := HubAPIClient.AttachScheme(corev1.AddToScheme); err != nil {
		return false, err
	}
	if err := HubAPIClient.AttachScheme(rbacv1.AddToScheme); err != nil {
		return false, err
	}

	serviceAccount := &corev1.ServiceAccount{}
	err := HubAPIClient.Get(context.Background(), types.NamespacedName{
		Name: ranparam.QuerierServiceAccountName, Namespace: ranparam.OpenshiftMonitoringNamespace,
	}, serviceAccount)
	serviceAccountMissing := apierrors.IsNotFound(err)
	if err != nil && !serviceAccountMissing {
		return false, err
	}

	roleBinding := &rbacv1.ClusterRoleBinding{}
	err = HubAPIClient.Get(context.Background(), types.NamespacedName{Name: ranparam.QuerierCRBName}, roleBinding)
	roleBindingMissing := apierrors.IsNotFound(err)
	if err != nil && !roleBindingMissing {
		return false, err
	}
	if serviceAccountMissing != roleBindingMissing {
		return false, fmt.Errorf("ACM metrics access has an incomplete querier ServiceAccount/ClusterRoleBinding setup")
	}

	return serviceAccountMissing, nil
}

func redeployACMSpokeMetricExists(api prometheusv1.API, spokeName string) (bool, error) {
	result, _, err := api.Query(context.Background(),
		fmt.Sprintf("cluster_infrastructure_provider{cluster=%q}", spokeName), time.Now())
	if err != nil {
		return false, err
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return false, fmt.Errorf("unexpected ACM metrics result %T", result)
	}

	return len(vector) > 0, nil
}

func redeployACMSpokeAlertExists(api *alertmanagerv2.AlertmanagerAPI, testID string) (bool, error) {
	params := alertmanageralert.NewGetAlertsParamsWithTimeout(30 * time.Second).
		WithFilter([]string{fmt.Sprintf("eco_gotests_test_id=%q", testID)})
	response, err := api.Alert.GetAlerts(params)
	if err != nil {
		return false, err
	}

	return len(response.Payload) > 0, nil
}

// redeployACMSpokeAlertUpdatedAfter reports whether ACM received a probe alert update after timestamp.
func redeployACMSpokeAlertUpdatedAfter(api *alertmanagerv2.AlertmanagerAPI, testID string, timestamp time.Time) (bool, error) {
	params := alertmanageralert.NewGetAlertsParamsWithTimeout(30 * time.Second).
		WithFilter([]string{fmt.Sprintf("eco_gotests_test_id=%q", testID)})
	response, err := api.Alert.GetAlerts(params)
	if err != nil {
		return false, err
	}

	for _, alert := range response.Payload {
		if alert.UpdatedAt == nil {
			return false, fmt.Errorf("ACM alert %q has no updatedAt timestamp", testID)
		}
		if time.Time(*alert.UpdatedAt).After(timestamp) {
			return true, nil
		}
	}

	return false, nil
}
