import app.k8s_scaler as kk

k = kk.RayClusterScaler()

print(k.scale_decr("raycluster-kuberay", "workergroup"))
