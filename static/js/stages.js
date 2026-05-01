// ============= 阶段管理模块 =============

async function loadStages() {
    try {
        const data = await apiCall('/stages', 'GET');
        const tbody = document.getElementById('stages-table-body');
        tbody.innerHTML = '';
        for (const stage of data) {
            const row = tbody.insertRow();
            row.insertCell().textContent = stage.name;
            row.insertCell().textContent = stage.description || '-';
            row.insertCell().textContent = stage.input_type;
            row.insertCell().textContent = stage.output_type;

            const actions = row.insertCell();
            actions.innerHTML = `
                <div class="btn-group">
                    <button class="btn btn-info btn-sm" onclick="showStageDetail('${escapeHtml(stage.name)}')">详情</button>
                    <button class="btn btn-secondary btn-sm" onclick="editStage('${escapeHtml(stage.name)}')">编辑</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteStage('${escapeHtml(stage.name)}')">删除</button>
                </div>
            `;
        }
    } catch (e) {
        console.error(e);
        showToast('加载阶段失败: ' + e.message, 'error');
    }
}

function openStageModal(prefill = null) {
    document.getElementById('stage-name').value = prefill ? prefill.name : '';
    document.getElementById('stage-handler').value = prefill ? prefill.handler : '';
    document.getElementById('stage-input-type').value = prefill ? prefill.input_type : '';
    document.getElementById('stage-output-type').value = prefill ? prefill.output_type : '';
    document.getElementById('stage-desc').value = prefill ? (prefill.description || '') : '';
    document.getElementById('stage-model').value = prefill ? (prefill.model_name || '') : '';
    document.getElementById('stage-deps').value = prefill ? (prefill.dependencies || []).join(', ') : '';
    document.getElementById('stage-config').value = prefill ? JSON.stringify(prefill.config || {}) : '{}';
    document.getElementById('stage-input-schema').value = prefill ? JSON.stringify(prefill.input_schema || {}) : '';
    document.getElementById('stage-output-schema').value = prefill ? JSON.stringify(prefill.output_schema || {}) : '';
    document.getElementById('stage-runtime').value = prefill ? JSON.stringify(prefill.runtime_env || {}) : '{}';
    document.getElementById('stage-can-split').value = prefill ? String(prefill.can_split || false) : 'false';
    document.getElementById('stage-is-deployable').value = prefill ? String(prefill.is_deployable !== false) : 'true';

    const nameInput = document.getElementById('stage-name');
    nameInput.readOnly = !!prefill;
    nameInput.style.backgroundColor = prefill ? '#f1f5f9' : '';

    document.getElementById('stage-modal-title').textContent = prefill ? '编辑阶段' : '添加阶段';
    openModal('stage-modal');
}

async function createStage() {
    const isEdit = document.getElementById('stage-modal-title').textContent === '编辑阶段';
    const name = document.getElementById('stage-name').value.trim();
    const handler = document.getElementById('stage-handler').value.trim();
    const input_type = document.getElementById('stage-input-type').value.trim();
    const output_type = document.getElementById('stage-output-type').value.trim();

    if (!name || !handler || !input_type || !output_type) {
        showToast('请填写必填字段', 'error');
        return;
    }

    const dependencies = document.getElementById('stage-deps').value.split(',')
        .map(s => s.trim()).filter(s => s);

    let config = {};
    try { config = JSON.parse(document.getElementById('stage-config').value || '{}'); } catch(e) {}
    let input_schema = null;
    try { input_schema = JSON.parse(document.getElementById('stage-input-schema').value || 'null'); } catch(e) {}
    let output_schema = null;
    try { output_schema = JSON.parse(document.getElementById('stage-output-schema').value || 'null'); } catch(e) {}
    let runtime_env = {};
    try { runtime_env = JSON.parse(document.getElementById('stage-runtime').value || '{}'); } catch(e) {}

    const body = {
        name, handler, input_type, output_type,
        description: document.getElementById('stage-desc').value,
        model_name: document.getElementById('stage-model').value || null,
        dependencies, config, input_schema, output_schema, runtime_env,
        can_split: document.getElementById('stage-can-split').value === 'true',
        is_deployable: document.getElementById('stage-is-deployable').value === 'true'
    };

    try {
        if (isEdit) {
            await apiCall(`/stages/${name}`, 'PUT', body);
            showToast('阶段更新成功');
        } else {
            await apiCall('/stages', 'POST', body);
            showToast('阶段创建成功');
        }
        closeModal('stage-modal');
        loadStages();
    } catch (e) {
        showToast((isEdit ? '更新' : '创建') + '阶段失败: ' + e.message, 'error');
    }
}

async function editStage(name) {
    try {
        const data = await apiCall('/stages', 'GET');
        const stage = data.find(s => s.name === name);
        if (!stage) {
            showToast('阶段不存在', 'error');
            return;
        }
        openStageModal(stage);
    } catch (e) {
        showToast('加载阶段失败: ' + e.message, 'error');
    }
}

async function deleteStage(name) {
    if (!confirmDelete(name)) return;
    try {
        await apiCall(`/stages/${name}`, 'DELETE');
        showToast('阶段已删除');
        loadStages();
    } catch (e) {
        showToast('删除阶段失败: ' + e.message, 'error');
    }
}

async function showStageDetail(name) {
    try {
        const data = await apiCall('/stages', 'GET');
        const stage = data.find(s => s.name === name);
        if (!stage) {
            showToast('阶段不存在', 'error');
            return;
        }

        let deployConfig = null;
        try {
            const deployData = await apiCall('/deployments', 'GET');
            deployConfig = deployData.find(d => d.stage_name === name);
        } catch (e) {}

        const content = document.getElementById('stage-detail-content');
        content.innerHTML = `
            <div class="detail-item">
                <div class="detail-label">阶段名称</div>
                <div class="detail-value">${name}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">描述</div>
                <div class="detail-value">${stage.description || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Handler</div>
                <div class="detail-value"><code>${stage.handler}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">输入类型</div>
                <div class="detail-value">${stage.input_type}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">输出类型</div>
                <div class="detail-value">${stage.output_type}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">模型名称</div>
                <div class="detail-value">${stage.model_name || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">依赖</div>
                <div class="detail-value">${(stage.dependencies || []).join(', ') || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">配置</div>
                <div class="detail-value"><code>${JSON.stringify(stage.config || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">输入格式 (Schema)</div>
                <div class="detail-value"><code>${JSON.stringify(stage.input_schema || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">输出格式 (Schema)</div>
                <div class="detail-value"><code>${JSON.stringify(stage.output_schema || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">运行环境</div>
                <div class="detail-value"><code>${JSON.stringify(stage.runtime_env || {}, null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">可拆分</div>
                <div class="detail-value">${stage.can_split ? '是' : '否'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">可部署</div>
                <div class="detail-value">${stage.is_deployable ? '是' : '否'}</div>
            </div>
            ${deployConfig ? `
                <div class="detail-item" style="margin-top: 12px; padding-top:12px; border-top: 2px solid #f1f5f9;">
                    <div class="detail-label" style="font-size:14px; color:#334155;">关联的部署配置</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">允许层级</div>
                    <div class="detail-value">${(deployConfig.allowed_tiers || []).join(', ')}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">资源需求</div>
                    <div class="detail-value"><code>${JSON.stringify(deployConfig.resources || {}, null, 2)}</code></div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">副本数</div>
                    <div class="detail-value">${deployConfig.replicas}</div>
                </div>
            ` : ''}
        `;
        openModal('stage-detail-modal');
    } catch (e) {
        showToast('获取详情失败: ' + e.message, 'error');
    }
}

// ============= 代码上传 =============

function openStageUploadModal() {
    document.getElementById('stage-upload-file').value = '';
    document.getElementById('stage-upload-result').innerHTML = '';
    loadUploadedStageFiles();
    openModal('stage-upload-modal');
}

async function uploadStageCode() {
    const fileInput = document.getElementById('stage-upload-file');
    if (!fileInput.files || !fileInput.files[0]) {
        showToast('请选择文件', 'error');
        return;
    }
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    try {
        const result = await apiUpload('/stages/upload', formData);
        const resultDiv = document.getElementById('stage-upload-result');
        resultDiv.innerHTML = `
            <div class="file-item" style="margin-top:12px; background:#d1fae5;">
                <div>
                    <div class="file-item-name">${escapeHtml(result.filename)}</div>
                    <div class="file-item-meta">模块: ${escapeHtml(result.module_name)} | ${result.message}</div>
                </div>
            </div>
        `;
        showToast('代码上传成功');
        loadUploadedStageFiles();
    } catch (e) {
        showToast('上传失败: ' + e.message, 'error');
    }
}

async function loadUploadedStageFiles() {
    try {
        const data = await apiCall('/stages/upload', 'GET');
        const container = document.getElementById('stage-uploaded-list');
        if (!data.files || data.files.length === 0) {
            container.innerHTML = '<div style="color:#94a3b8; font-size:13px; padding:8px 0;">暂无已上传的文件</div>';
            return;
        }
        container.innerHTML = data.files.map(f => `
            <div class="file-item">
                <div>
                    <div class="file-item-name">${escapeHtml(f.filename)}</div>
                    <div class="file-item-meta">模块: ${escapeHtml(f.module_name)} | ${formatBytes(f.file_size)}</div>
                </div>
                <button class="btn btn-danger btn-sm" onclick="deleteUploadedStageFile('${escapeHtml(f.filename)}')">删除</button>
            </div>
        `).join('');
    } catch (e) {
        console.error(e);
    }
}

async function deleteUploadedStageFile(filename) {
    if (!confirmDelete(filename)) return;
    try {
        await apiCall(`/stages/upload/${filename}`, 'DELETE');
        showToast('文件已删除');
        loadUploadedStageFiles();
    } catch (e) {
        showToast('删除失败: ' + e.message, 'error');
    }
}
