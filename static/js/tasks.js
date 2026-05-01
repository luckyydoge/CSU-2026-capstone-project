// ============= 任务提交模块 =============
let currentTaskId = null;

async function loadSubmitOptions() {
    try {
        const appsData = await apiCall('/applications', 'GET');
        const appSelect = document.getElementById('submit-app');
        appSelect.innerHTML = '<option value="">请选择应用</option>';
        for (const app of appsData) {
            const opt = document.createElement('option');
            opt.value = app.name;
            opt.textContent = app.name;
            appSelect.appendChild(opt);
        }

        const strategiesData = await apiCall('/strategies', 'GET');
        const strategySelect = document.getElementById('submit-strategy');
        strategySelect.innerHTML = '<option value="">请选择策略</option>';
        for (const strategy of strategiesData) {
            const opt = document.createElement('option');
            opt.value = strategy.name;
            opt.textContent = strategy.name;
            strategySelect.appendChild(opt);
        }
    } catch (e) {
        console.error(e);
    }
}

async function submitTask() {
    const appName = document.getElementById('submit-app').value.trim();
    const strategyName = document.getElementById('submit-strategy').value.trim();
    const fileInput = document.getElementById('submit-file');

    if (!appName || !strategyName) {
        showToast('请选择应用和策略', 'error');
        return;
    }

    try {
        let fileId = null;

        if (fileInput.files && fileInput.files[0]) {
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            const uploadResult = await apiUpload('/files/upload', formData);
            fileId = uploadResult.file_id;
        }

        const body = {
            app_name: appName,
            strategy_name: strategyName,
            input_data_uri: fileId ? JSON.stringify({ file_id: fileId }) : null
        };

        const taskData = await apiCall('/tasks', 'POST', body);
        currentTaskId = taskData.task_id;

        showToast('任务提交成功');

        const statusArea = document.getElementById('task-status-area');
        statusArea.innerHTML = `<div><strong>任务ID:</strong> ${currentTaskId}</div><div><strong>状态:</strong> <span class="badge badge-warning">运行中</span></div>`;
        document.getElementById('task-result-area').innerHTML = '等待结果...';

        pollTaskStatus(currentTaskId);
    } catch (e) {
        showToast('任务提交失败: ' + e.message, 'error');
    }
}

async function pollTaskStatus(taskId) {
    const pollInterval = setInterval(async () => {
        try {
            const data = await apiCall(`/tasks/${taskId}`, 'GET');
            const statusArea = document.getElementById('task-status-area');

            if (data.status === 'completed') {
                clearInterval(pollInterval);
                statusArea.innerHTML = `
                    <div><strong>任务ID:</strong> ${taskId}</div>
                    <div><strong>状态:</strong> <span class="badge badge-success">已完成</span></div>
                `;
                document.getElementById('task-result-area').innerHTML = `
                    <div class="detail-item">
                        <div class="detail-label">结果</div>
                        <div class="detail-value"><code>${JSON.stringify(data.final_output || {}, null, 2)}</code></div>
                    </div>
                `;
            } else if (data.status === 'failed') {
                clearInterval(pollInterval);
                statusArea.innerHTML = `
                    <div><strong>任务ID:</strong> ${taskId}</div>
                    <div><strong>状态:</strong> <span class="badge badge-error">失败</span></div>
                `;
                document.getElementById('task-result-area').innerHTML = `
                    <div class="detail-item">
                        <div class="detail-label">错误</div>
                        <div class="detail-value">${data.final_output || '未知错误'}</div>
                    </div>
                `;
            }
        } catch (e) {
            console.error(e);
        }
    }, 2000);
}

// ============= 任务记录模块 =============
let allTasks = [];

async function loadRecords() {
    try {
        const data = await apiCall('/tasks', 'GET');
        allTasks = Array.isArray(data) ? data : Object.values(data);
        renderRecords(allTasks);
    } catch (e) {
        console.error(e);
        showToast('加载任务记录失败: ' + e.message, 'error');
    }
}

function renderRecords(tasks) {
    const tbody = document.getElementById('records-table-body');
    tbody.innerHTML = '';
    for (const task of tasks) {
        const row = tbody.insertRow();
        row.insertCell().textContent = task.task_id || '-';
        row.insertCell().textContent = task.app_name || '-';
        row.insertCell().textContent = task.strategy_name || '-';

        const statusCell = row.insertCell();
        statusCell.innerHTML = statusBadge(task.status);

        row.insertCell().textContent = formatBeijingTime(task.created_at);

        const actions = row.insertCell();
        actions.innerHTML = `
            <div class="btn-group">
                <button class="btn btn-info btn-sm" onclick="viewTaskTrace('${task.task_id}')">追踪</button>
                <button class="btn btn-danger btn-sm" onclick="deleteTask('${task.task_id}')">删除</button>
            </div>
        `;
    }
}

function filterRecords() {
    const status = document.getElementById('records-filter-status').value;
    const filtered = status ? allTasks.filter(t => t.status === status) : allTasks;
    renderRecords(filtered);
}

function viewTaskTrace(taskId) {
    document.getElementById('trace-task-id').value = taskId;
    document.querySelector('.nav-item[data-module="trace"]').click();
}

async function deleteTask(taskId) {
    if (!confirmDelete(taskId)) return;
    try {
        await apiCall(`/tasks/${taskId}`, 'DELETE');
        showToast('任务已删除');
        loadRecords();
    } catch (e) {
        showToast('删除任务失败: ' + e.message, 'error');
    }
}

// ============= 任务追踪模块 =============
async function loadTrace() {
    const taskId = document.getElementById('trace-task-id').value.trim();
    if (!taskId) {
        showToast('请输入任务ID', 'error');
        return;
    }

    try {
        const data = await apiCall(`/tasks/${taskId}`, 'GET');
        let traceData = null;
        try {
            traceData = await apiCall(`/tasks/${taskId}/traces`, 'GET');
        } catch (e) {}

        const traceContent = document.getElementById('trace-content');

        let traceHtml = `
            <div class="card" style="margin-top: 16px;">
                <div class="detail-item">
                    <div class="detail-label">任务ID</div>
                    <div class="detail-value" style="font-family:monospace;">${taskId}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">应用</div>
                    <div class="detail-value">${data.app_name || '-'}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">策略</div>
                    <div class="detail-value">${data.strategy_name || '-'}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">状态</div>
                    <div class="detail-value">${statusBadge(data.status)}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">创建时间</div>
                    <div class="detail-value">${formatBeijingTime(data.created_at)}</div>
                </div>
                ${data.completed_at ? `
                <div class="detail-item">
                    <div class="detail-label">完成时间</div>
                    <div class="detail-value">${formatBeijingTime(data.completed_at)}</div>
                </div>` : ''}
        `;

        if (traceData && traceData.length > 0) {
            // 计算总执行时间和总传输时间
            const totalTime = traceData.reduce((sum, s) => sum + (s.execution_time_ms || 0), 0);
            const totalTransfer = traceData.reduce((sum, s) => sum + (s.transfer_time_ms || 0), 0);

            traceHtml += `
                <div class="detail-item" style="margin-top: 12px; padding-top:12px; border-top: 2px solid #f1f5f9;">
                    <div class="detail-label" style="font-size:14px; color:#334155;">执行路径 (${traceData.length} 步, 总耗时 ${totalTime.toFixed(1)}ms, 传输 ${totalTransfer.toFixed(1)}ms)</div>
                </div>
            `;

            for (const step of traceData) {
                traceHtml += `
                    <div class="detail-item">
                        <div class="detail-label">步骤 ${step.step_index} - ${escapeHtml(step.stage_name)}</div>
                        <div class="detail-value">
                            <div>节点: ${escapeHtml(step.node_id || '-')} (${escapeHtml(step.node_tier || '-')})</div>
                            <div>执行时间: ${step.execution_time_ms ? step.execution_time_ms.toFixed(1) + 'ms' : '-'} | 传输时间: ${step.transfer_time_ms ? step.transfer_time_ms.toFixed(1) + 'ms' : '-'}</div>
                            <div>输入: ${formatBytes(step.input_size_bytes)} → 输出: ${formatBytes(step.output_size_bytes)}</div>
                            ${step.cpu_percent != null ? `<div>CPU: ${step.cpu_percent.toFixed(1)}% | 内存: ${step.memory_mb ? step.memory_mb + 'MB' : '-'}</div>` : ''}
                            ${step.error_msg ? `<div style="color:#ef4444;">错误: ${escapeHtml(step.error_msg)}</div>` : ''}
                        </div>
                    </div>
                `;
            }
        }

        if (data.final_output) {
            traceHtml += `
                <div class="detail-item" style="margin-top: 12px; padding-top:12px; border-top: 2px solid #f1f5f9;">
                    <div class="detail-label" style="font-size:14px; color:#334155;">最终结果</div>
                </div>
                <div class="detail-item">
                    <div class="detail-value"><code>${JSON.stringify(data.final_output, null, 2)}</code></div>
                </div>
            `;
        }

        // 导出按钮
        traceHtml += `
            <div style="margin-top:16px; display:flex; gap:12px; justify-content:flex-end;">
                <button class="btn btn-secondary" onclick="exportTaskTraceJSON('${taskId}')">导出 JSON</button>
                <button class="btn btn-secondary" onclick="exportTaskTraceCSV('${taskId}')">导出 CSV</button>
            </div>
        `;

        traceHtml += '</div>';
        traceContent.innerHTML = traceHtml;

        // 保存数据供导出
        window._currentTrace = { task: data, traces: traceData };
    } catch (e) {
        showToast('加载追踪信息失败: ' + e.message, 'error');
    }
}

function exportTaskTraceJSON(taskId) {
    if (!window._currentTrace) {
        showToast('无追踪数据可导出', 'error');
        return;
    }
    exportJSON(window._currentTrace, `task_${taskId}_trace.json`);
}

function exportTaskTraceCSV(taskId) {
    if (!window._currentTrace || !window._currentTrace.traces) {
        showToast('无追踪数据可导出', 'error');
        return;
    }
    const headers = ['step_index', 'stage_name', 'node_id', 'node_tier', 'execution_time_ms', 'transfer_time_ms', 'input_size_bytes', 'output_size_bytes', 'cpu_percent', 'memory_mb', 'error_msg'];
    const rows = window._currentTrace.traces.map(s => [
        s.step_index, s.stage_name || '', s.node_id || '', s.node_tier || '',
        s.execution_time_ms || '', s.transfer_time_ms || '',
        s.input_size_bytes || '', s.output_size_bytes || '',
        s.cpu_percent || '', s.memory_mb || '', s.error_msg || ''
    ]);
    exportCSV(headers, rows, `task_${taskId}_trace.csv`);
}
