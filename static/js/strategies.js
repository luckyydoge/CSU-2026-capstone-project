// ============= 策略管理模块 =============

async function loadStrategies() {
    try {
        const data = await apiCall('/strategies', 'GET');
        const tbody = document.getElementById('strategies-table-body');
        tbody.innerHTML = '';
        for (const strategy of data) {
            const row = tbody.insertRow();
            row.insertCell().textContent = strategy.name;
            row.insertCell().textContent = strategy.strategy_type || '-';
            row.insertCell().textContent = strategy.description || '-';

            const actions = row.insertCell();
            actions.innerHTML = `
                <div class="btn-group">
                    <button class="btn btn-info btn-sm" onclick="showStrategyDetail('${escapeHtml(strategy.name)}')">详情</button>
                    <button class="btn btn-secondary btn-sm" onclick="editStrategy('${escapeHtml(strategy.name)}')">编辑</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteStrategy('${escapeHtml(strategy.name)}')">删除</button>
                </div>
            `;
        }
    } catch (e) {
        console.error(e);
        showToast('加载策略失败: ' + e.message, 'error');
    }
}

function openStrategyModal(prefill = null) {
    document.getElementById('strategy-name').value = prefill ? prefill.name : '';
    document.getElementById('strategy-type').value = prefill ? prefill.strategy_type : 'path';
    document.getElementById('strategy-handler').value = prefill ? prefill.handler : '';
    document.getElementById('strategy-desc').value = prefill ? (prefill.description || '') : '';
    document.getElementById('strategy-params').value = prefill ? JSON.stringify(prefill.config || {}) : '{}';

    const nameInput = document.getElementById('strategy-name');
    nameInput.readOnly = !!prefill;
    nameInput.style.backgroundColor = prefill ? '#f1f5f9' : '';

    document.getElementById('strategy-modal-title').textContent = prefill ? '编辑策略' : '添加策略';
    openModal('strategy-modal');
}

async function createStrategy() {
    const isEdit = document.getElementById('strategy-modal-title').textContent === '编辑策略';
    const name = document.getElementById('strategy-name').value.trim();
    const strategy_type = document.getElementById('strategy-type').value.trim();
    const handler = document.getElementById('strategy-handler').value.trim();

    if (!name || !strategy_type || !handler) {
        showToast('请填写必填字段', 'error');
        return;
    }

    let params = {};
    try {
        params = JSON.parse(document.getElementById('strategy-params').value || '{}');
    } catch (e) {
        showToast('策略参数JSON格式无效', 'error');
        return;
    }

    const body = {
        name, strategy_type, handler,
        description: document.getElementById('strategy-desc').value,
        config: params
    };

    try {
        if (isEdit) {
            await apiCall(`/strategies/${name}`, 'PUT', body);
            showToast('策略更新成功');
        } else {
            await apiCall('/strategies', 'POST', body);
            showToast('策略创建成功');
        }
        closeModal('strategy-modal');
        loadStrategies();
    } catch (e) {
        showToast((isEdit ? '更新' : '创建') + '策略失败: ' + e.message, 'error');
    }
}

async function editStrategy(name) {
    try {
        const data = await apiCall('/strategies', 'GET');
        const strategy = data.find(s => s.name === name);
        if (!strategy) {
            showToast('策略不存在', 'error');
            return;
        }
        openStrategyModal(strategy);
    } catch (e) {
        showToast('加载策略失败: ' + e.message, 'error');
    }
}

async function deleteStrategy(name) {
    if (!confirmDelete(name)) return;
    try {
        await apiCall(`/strategies/${name}`, 'DELETE');
        showToast('策略已删除');
        loadStrategies();
    } catch (e) {
        showToast('删除策略失败: ' + e.message, 'error');
    }
}

async function showStrategyDetail(name) {
    try {
        const data = await apiCall('/strategies', 'GET');
        const strategy = data.find(s => s.name === name);
        if (!strategy) {
            showToast('策略不存在', 'error');
            return;
        }

        const content = document.getElementById('strategy-detail-content');
        content.innerHTML = `
            <div class="detail-item">
                <div class="detail-label">策略名称</div>
                <div class="detail-value">${name}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">策略类型</div>
                <div class="detail-value">${strategy.strategy_type || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Handler</div>
                <div class="detail-value"><code>${strategy.handler}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">描述</div>
                <div class="detail-value">${strategy.description || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">策略参数</div>
                <div class="detail-value"><code>${JSON.stringify(strategy.config || {}, null, 2)}</code></div>
            </div>
        `;
        openModal('strategy-detail-modal');
    } catch (e) {
        showToast('获取详情失败: ' + e.message, 'error');
    }
}

// ============= 策略代码上传 =============

function openStrategyUploadModal() {
    document.getElementById('strategy-upload-file').value = '';
    document.getElementById('strategy-upload-result').innerHTML = '';
    loadUploadedStrategyFiles();
    openModal('strategy-upload-modal');
}

async function uploadStrategyCode() {
    const fileInput = document.getElementById('strategy-upload-file');
    if (!fileInput.files || !fileInput.files[0]) {
        showToast('请选择文件', 'error');
        return;
    }
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    try {
        const result = await apiUpload('/strategies/upload', formData);
        const resultDiv = document.getElementById('strategy-upload-result');
        resultDiv.innerHTML = `
            <div class="file-item" style="margin-top:12px; background:#d1fae5;">
                <div>
                    <div class="file-item-name">${escapeHtml(result.filename)}</div>
                    <div class="file-item-meta">模块: ${escapeHtml(result.module_name)} | ${result.message}</div>
                </div>
            </div>
        `;
        showToast('策略代码上传成功');
        loadUploadedStrategyFiles();
    } catch (e) {
        showToast('上传失败: ' + e.message, 'error');
    }
}

async function loadUploadedStrategyFiles() {
    try {
        const data = await apiCall('/strategies/upload', 'GET');
        const container = document.getElementById('strategy-uploaded-list');
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
                <button class="btn btn-danger btn-sm" onclick="deleteUploadedStrategyFile('${escapeHtml(f.filename)}')">删除</button>
            </div>
        `).join('');
    } catch (e) {
        console.error(e);
    }
}

async function deleteUploadedStrategyFile(filename) {
    if (!confirmDelete(filename)) return;
    try {
        await apiCall(`/strategies/upload/${filename}`, 'DELETE');
        showToast('文件已删除');
        loadUploadedStrategyFiles();
    } catch (e) {
        showToast('删除失败: ' + e.message, 'error');
    }
}
