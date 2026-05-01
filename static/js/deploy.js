// ============= 部署配置模块 =============

async function loadDeployments() {
    try {
        const data = await apiCall('/deployments', 'GET');
        const tbody = document.getElementById('deploy-table-body');
        tbody.innerHTML = '';
        for (const deploy of data) {
            const row = tbody.insertRow();
            row.insertCell().textContent = deploy.stage_name;
            row.insertCell().textContent = deploy.description || '-';
            row.insertCell().textContent = (deploy.allowed_tiers || []).join(', ');
            row.insertCell().textContent = deploy.replicas || 1;

            const actions = row.insertCell();
            actions.innerHTML = `
                <div class="btn-group">
                    <button class="btn btn-info btn-sm" onclick="showDeployDetail('${escapeHtml(deploy.stage_name)}')">详情</button>
                    <button class="btn btn-secondary btn-sm" onclick="editDeployment('${escapeHtml(deploy.stage_name)}')">编辑</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteDeployment('${escapeHtml(deploy.stage_name)}')">删除</button>
                </div>
            `;
        }
    } catch (e) {
        console.error(e);
        showToast('加载部署配置失败: ' + e.message, 'error');
    }
}

async function openDeployModal(prefill = null) {
    document.getElementById('deploy-stage-name').innerHTML = '<option value="">请选择阶段</option>';
    document.getElementById('deploy-desc').value = prefill ? (prefill.description || '') : '';
    document.getElementById('deploy-allowed-tiers').value = prefill ? JSON.stringify(prefill.allowed_tiers) : '["end", "edge", "cloud"]';
    document.getElementById('deploy-resources').value = prefill ? JSON.stringify(prefill.resources) : '{"cpu_cores": 0.5, "memory_mb": 256, "gpu_count": 0}';
    document.getElementById('deploy-replicas').value = prefill ? prefill.replicas : '1';
    document.getElementById('deploy-node-affinity').value = prefill ? JSON.stringify(prefill.node_affinity || {}) : '{}';
    document.getElementById('deploy-proximity').value = prefill ? JSON.stringify(prefill.proximity || {}) : '{}';

    const stageSelect = document.getElementById('deploy-stage-name');
    const isEdit = !!prefill;
    try {
        const stagesData = await apiCall('/stages', 'GET');
        for (const stage of stagesData) {
            const opt = document.createElement('option');
            opt.value = stage.name;
            opt.textContent = stage.name;
            if (prefill && prefill.stage_name === stage.name) opt.selected = true;
            stageSelect.appendChild(opt);
        }
    } catch (e) {
        console.error(e);
    }

    stageSelect.disabled = isEdit;
    stageSelect.style.backgroundColor = isEdit ? '#f1f5f9' : '';
    document.getElementById('deploy-modal-title').textContent = isEdit ? '编辑部署配置' : '添加部署配置';
    openModal('deploy-modal');
}

async function createDeployment() {
    const isEdit = document.getElementById('deploy-modal-title').textContent === '编辑部署配置';
    const stage_name = document.getElementById('deploy-stage-name').value.trim();

    if (!stage_name) {
        showToast('请选择阶段', 'error');
        return;
    }

    let allowed_tiers, resources, node_affinity, proximity;
    try { allowed_tiers = JSON.parse(document.getElementById('deploy-allowed-tiers').value); }
    catch (e) { showToast('允许层级JSON格式无效', 'error'); return; }
    try { resources = JSON.parse(document.getElementById('deploy-resources').value); }
    catch (e) { showToast('资源需求JSON格式无效', 'error'); return; }
    try { node_affinity = JSON.parse(document.getElementById('deploy-node-affinity').value || '{}'); }
    catch (e) { showToast('节点亲和JSON格式无效', 'error'); return; }
    try { proximity = JSON.parse(document.getElementById('deploy-proximity').value || '{}'); }
    catch (e) { showToast('邻近部署JSON格式无效', 'error'); return; }

    const body = {
        stage_name,
        description: document.getElementById('deploy-desc').value,
        allowed_tiers, resources,
        replicas: parseInt(document.getElementById('deploy-replicas').value) || 1,
        node_affinity, proximity
    };

    try {
        if (isEdit) {
            await apiCall(`/deployments/${stage_name}`, 'PUT', body);
            showToast('部署配置更新成功');
        } else {
            await apiCall('/deployments', 'POST', body);
            showToast('部署配置创建成功');
        }
        closeModal('deploy-modal');
        loadDeployments();
    } catch (e) {
        showToast((isEdit ? '更新' : '创建') + '部署配置失败: ' + e.message, 'error');
    }
}

async function editDeployment(stageName) {
    try {
        const data = await apiCall('/deployments', 'GET');
        const deploy = data.find(d => d.stage_name === stageName);
        if (!deploy) {
            showToast('部署配置不存在', 'error');
            return;
        }
        openDeployModal(deploy);
    } catch (e) {
        showToast('加载部署配置失败: ' + e.message, 'error');
    }
}

async function deleteDeployment(stageName) {
    if (!confirmDelete(stageName)) return;
    try {
        await apiCall(`/deployments/${stageName}`, 'DELETE');
        showToast('部署配置已删除');
        loadDeployments();
    } catch (e) {
        showToast('删除部署配置失败: ' + e.message, 'error');
    }
}

async function showDeployDetail(stageName) {
    try {
        const data = await apiCall('/deployments', 'GET');
        const deploy = data.find(d => d.stage_name === stageName);
        if (!deploy) {
            showToast('部署配置不存在', 'error');
            return;
        }

        const content = document.getElementById('deploy-detail-content');
        content.innerHTML = `
            <div class="detail-item">
                <div class="detail-label">阶段名称</div>
                <div class="detail-value">${stageName}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">描述</div>
                <div class="detail-value">${deploy.description || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">允许层级</div>
                <div class="detail-value">${(deploy.allowed_tiers || []).join(', ')}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">资源需求</div>
                <div class="detail-value"><code>${JSON.stringify(deploy.resources || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">副本数</div>
                <div class="detail-value">${deploy.replicas}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">节点亲和</div>
                <div class="detail-value"><code>${JSON.stringify(deploy.node_affinity || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">邻近部署</div>
                <div class="detail-value"><code>${JSON.stringify(deploy.proximity || {}, null, 2)}</code></div>
            </div>
        `;
        openModal('deploy-detail-modal');
    } catch (e) {
        showToast('获取详情失败: ' + e.message, 'error');
    }
}
