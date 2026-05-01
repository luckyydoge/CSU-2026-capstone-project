// ============= 应用管理模块 =============
let allStagesList = [];

async function loadApps() {
    try {
        const stagesData = await apiCall('/stages', 'GET');
        allStagesList = stagesData.map(s => s.name);

        const data = await apiCall('/applications', 'GET');
        const tbody = document.getElementById('apps-table-body');
        tbody.innerHTML = '';
        for (const app of data) {
            const row = tbody.insertRow();
            row.insertCell().textContent = app.name || '-';
            row.insertCell().textContent = app.description || '-';
            row.insertCell().textContent = app.input_type || '-';
            row.insertCell().textContent = app.entry_stage || '-';

            const actions = row.insertCell();
            actions.innerHTML = `
                <div class="btn-group">
                    <button class="btn btn-info btn-sm" onclick="showAppDetail('${escapeHtml(app.name)}')">详情</button>
                    <button class="btn btn-secondary btn-sm" onclick="editApp('${escapeHtml(app.name)}')">编辑</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteApp('${escapeHtml(app.name)}')">删除</button>
                </div>
            `;
        }
    } catch (e) {
        console.error(e);
        showToast('加载应用失败: ' + e.message, 'error');
    }
}

async function openAppModal(prefill = null) {
    document.getElementById('app-name').value = prefill ? prefill.name : '';
    document.getElementById('app-input-type').value = prefill ? prefill.input_type : '';
    document.getElementById('app-desc').value = prefill ? (prefill.description || '') : '';
    document.getElementById('app-stages').value = prefill ? JSON.stringify(prefill.stages || []) : '[]';
    document.getElementById('app-edges').value = prefill ? JSON.stringify(prefill.edges || []) : '[]';
    document.getElementById('app-exit-stages').value = prefill ? JSON.stringify(prefill.exit_stages || []) : '[]';

    const nameInput = document.getElementById('app-name');
    nameInput.readOnly = !!prefill;
    nameInput.style.backgroundColor = prefill ? '#f1f5f9' : '';

    document.getElementById('app-modal-title').textContent = prefill ? '编辑应用' : '添加应用';

    const entrySelect = document.getElementById('app-entry-stage');
    entrySelect.innerHTML = '<option value="">请选择阶段</option>';
    try {
        const stagesData = await apiCall('/stages', 'GET');
        allStagesList = stagesData.map(s => s.name);
        for (const stageName of allStagesList) {
            const opt = document.createElement('option');
            opt.value = stageName;
            opt.textContent = stageName;
            if (prefill && prefill.entry_stage === stageName) opt.selected = true;
            entrySelect.appendChild(opt);
        }
    } catch (e) {
        console.error(e);
    }

    openModal('app-modal');
}

async function createApp() {
    const isEdit = document.getElementById('app-modal-title').textContent === '编辑应用';
    const name = document.getElementById('app-name').value.trim();
    const input_type = document.getElementById('app-input-type').value.trim();
    const entry_stage = document.getElementById('app-entry-stage').value.trim();

    if (!name || !input_type || !entry_stage) {
        showToast('请填写必填字段', 'error');
        return;
    }

    let stages, edges, exit_stages;
    try { stages = JSON.parse(document.getElementById('app-stages').value || '[]'); }
    catch (e) { showToast('阶段列表JSON格式无效', 'error'); return; }
    try { edges = JSON.parse(document.getElementById('app-edges').value || '[]'); }
    catch (e) { showToast('边列表JSON格式无效', 'error'); return; }
    try { exit_stages = JSON.parse(document.getElementById('app-exit-stages').value || '[]'); }
    catch (e) { showToast('出口阶段JSON格式无效', 'error'); return; }

    const body = {
        name, input_type,
        description: document.getElementById('app-desc').value,
        stages, edges, entry_stage, exit_stages
    };

    try {
        if (isEdit) {
            showToast('应用不支持修改名称和结构，请删除后重建', 'error');
            return;
        }
        await apiCall('/applications', 'POST', body);
        showToast('应用创建成功');
        closeModal('app-modal');
        loadApps();
    } catch (e) {
        showToast('创建应用失败: ' + e.message, 'error');
    }
}

async function editApp(name) {
    try {
        const data = await apiCall('/applications', 'GET');
        const app = data.find(a => a.name === name);
        if (!app) {
            showToast('应用不存在', 'error');
            return;
        }
        // 获取完整的应用信息（含 stages/edges）
        const fullApp = await apiCall(`/applications/${name}`, 'GET');
        openAppModal(fullApp);
    } catch (e) {
        showToast('加载应用失败: ' + e.message, 'error');
    }
}

async function deleteApp(name) {
    if (!confirmDelete(name)) return;
    try {
        await apiCall(`/applications/${name}`, 'DELETE');
        showToast('应用已删除');
        loadApps();
    } catch (e) {
        showToast('删除应用失败: ' + e.message, 'error');
    }
}

async function showAppDetail(name) {
    try {
        const app = await apiCall(`/applications/${name}`, 'GET');

        const content = document.getElementById('app-detail-content');
        content.innerHTML = `
            <div class="detail-item">
                <div class="detail-label">应用名称</div>
                <div class="detail-value">${app.name || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">应用ID</div>
                <div class="detail-value">${app.app_id || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">描述</div>
                <div class="detail-value">${app.description || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">输入类型</div>
                <div class="detail-value">${app.input_type || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">入口阶段</div>
                <div class="detail-value">${app.entry_stage || '-'}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">出口阶段</div>
                <div class="detail-value"><code>${JSON.stringify(app.exit_stages || [])}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">阶段列表</div>
                <div class="detail-value"><code>${JSON.stringify(app.stages || [])}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">边列表</div>
                <div class="detail-value"><code>${JSON.stringify(app.edges || [], null, 2)}</code></div>
            </div>
            <div class="detail-item">
                <div class="detail-label">DAG 拓扑图</div>
                <div class="dag-container">
                    <canvas id="dag-canvas-detail" class="dag-canvas" width="800" height="400"></canvas>
                </div>
            </div>
        `;
        openModal('app-detail-modal');

        setTimeout(() => drawDAG('dag-canvas-detail', app), 50);
    } catch (e) {
        showToast('获取详情失败: ' + e.message, 'error');
    }
}

// ============= DAG 可视化 =============

function drawDAG(canvasId, appData) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const stages = appData.stages || [];
    const edges = (appData.edges || []).map(e => ({
        from: e.from_stage,
        to: e.to_stage
    }));
    const entryStage = appData.entry_stage;
    const exitStages = appData.exit_stages || [];

    if (stages.length === 0) {
        ctx.fillStyle = '#94a3b8';
        ctx.font = '14px sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('无阶段数据', canvas.width / 2, canvas.height / 2);
        return;
    }

    // 拓扑排序分层
    const layers = topoLayers(stages, edges);

    // 布局参数
    const nodeW = 140, nodeH = 56, padX = 60, padY = 40, startX = 40, startY = 40;
    const maxPerRow = Math.max(...layers.map(l => l.length));
    canvas.width = Math.max(800, layers.length * (nodeW + padX) + startX * 2);
    canvas.height = Math.max(300, maxPerRow * (nodeH + padY) + startY * 2);

    // 计算节点坐标
    const positions = {};
    layers.forEach((layer, col) => {
        const totalH = layer.length * (nodeH + padY) - padY;
        const offsetY = (canvas.height - totalH) / 2;
        layer.forEach((node, row) => {
            positions[node] = {
                x: startX + col * (nodeW + padX),
                y: offsetY + row * (nodeH + padY),
                cx: startX + col * (nodeW + padX) + nodeW / 2,
                cy: offsetY + row * (nodeH + padY) + nodeH / 2
            };
        });
    });

    // 绘制边（箭头）
    ctx.strokeStyle = '#94a3b8';
    ctx.lineWidth = 2;
    for (const edge of edges) {
        const from = positions[edge.from];
        const to = positions[edge.to];
        if (!from || !to) continue;

        const x1 = from.x + nodeW, y1 = from.cy;
        const x2 = to.x, y2 = to.cy;

        ctx.beginPath();
        ctx.moveTo(x1, y1);
        // 贝塞尔曲线
        const cpx = (x1 + x2) / 2;
        ctx.bezierCurveTo(cpx, y1, cpx, y2, x2, y2);
        ctx.stroke();

        // 箭头
        const angle = Math.atan2(y2 - (y2 + y1) / 2, x2 - cpx);
        const arrowLen = 10;
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(x2 - arrowLen * Math.cos(angle - 0.4), y2 - arrowLen * Math.sin(angle - 0.4));
        ctx.lineTo(x2 - arrowLen * Math.cos(angle + 0.4), y2 - arrowLen * Math.sin(angle + 0.4));
        ctx.closePath();
        ctx.fillStyle = '#94a3b8';
        ctx.fill();
    }

    // 绘制节点
    for (const stage of stages) {
        const pos = positions[stage];
        if (!pos) continue;

        const isEntry = stage === entryStage;
        const isExit = exitStages.includes(stage);

        // 背景
        ctx.fillStyle = '#ffffff';
        ctx.strokeStyle = isEntry ? '#10b981' : isExit ? '#ef4444' : '#3b82f6';
        ctx.lineWidth = isEntry || isExit ? 3 : 2;
        roundRect(ctx, pos.x, pos.y, nodeW, nodeH, 10);
        ctx.fill();
        ctx.stroke();

        // 文字
        ctx.fillStyle = '#0f172a';
        ctx.font = 'bold 13px sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(truncate(stage, 14), pos.cx, pos.cy - 8);

        // 标签
        ctx.font = '11px sans-serif';
        ctx.fillStyle = '#64748b';
        const tag = isEntry ? '[入口]' : isExit ? '[出口]' : '';
        if (tag) ctx.fillText(tag, pos.cx, pos.cy + 12);
    }
}

function topoLayers(stages, edges) {
    const inDeg = {};
    const adj = {};
    for (const s of stages) { inDeg[s] = 0; adj[s] = []; }
    for (const e of edges) {
        if (adj[e.from] !== undefined) {
            adj[e.from].push(e.to);
            inDeg[e.to] = (inDeg[e.to] || 0) + 1;
        }
    }

    const layers = [];
    let queue = stages.filter(s => inDeg[s] === 0);
    const visited = new Set();

    while (queue.length > 0) {
        layers.push([...queue]);
        const next = [];
        for (const node of queue) {
            visited.add(node);
            for (const neighbor of (adj[node] || [])) {
                inDeg[neighbor]--;
                if (inDeg[neighbor] === 0 && !visited.has(neighbor)) {
                    next.push(neighbor);
                }
            }
        }
        queue = next;
    }

    // 处理环中未访问的节点
    const remaining = stages.filter(s => !visited.has(s));
    if (remaining.length > 0) layers.push(remaining);

    return layers;
}

function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
}

function truncate(str, max) {
    return str.length > max ? str.slice(0, max - 1) + '…' : str;
}
