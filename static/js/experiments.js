// ============= 实验管理模块 =============

async function loadExperiments() {
    try {
        const data = await apiCall('/experiments', 'GET');
        const tbody = document.getElementById('experiments-table-body');
        tbody.innerHTML = '';
        for (const exp of data) {
            const row = tbody.insertRow();
            row.insertCell().textContent = exp.name || '-';
            row.insertCell().textContent = exp.app_name || '-';
            row.insertCell().textContent = (exp.strategy_group || []).join(', ') || '-';
            row.insertCell().textContent = exp.task_count != null ? exp.task_count : '-';

            const statusCell = row.insertCell();
            statusCell.innerHTML = statusBadge(exp.status);

            row.insertCell().textContent = formatBeijingTime(exp.created_at);

            const actions = row.insertCell();
            actions.innerHTML = `
                <div class="btn-group">
                    <button class="btn btn-info btn-sm" onclick="showExperimentReport('${exp.exp_id}')">报告</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteExperiment('${exp.exp_id}')">删除</button>
                </div>
            `;
        }
    } catch (e) {
        console.error(e);
        showToast('加载实验失败: ' + e.message, 'error');
    }
}

async function openExperimentModal() {
    document.getElementById('exp-name').value = '';
    document.getElementById('exp-rounds').value = '1';
    document.getElementById('exp-max-retries').value = '1';

    // 重置输入区域为单个文件输入
    resetExperimentInputs();

    // 加载应用选项
    const appSelect = document.getElementById('exp-app');
    appSelect.innerHTML = '<option value="">请选择应用</option>';
    try {
        const apps = await apiCall('/applications', 'GET');
        for (const app of apps) {
            const opt = document.createElement('option');
            opt.value = app.name;
            opt.textContent = app.name;
            appSelect.appendChild(opt);
        }
    } catch (e) {
        console.error(e);
    }

    // 加载策略多选
    const checkboxGroup = document.getElementById('exp-strategies-checkbox');
    checkboxGroup.innerHTML = '';
    try {
        const strategies = await apiCall('/strategies', 'GET');
        for (const s of strategies) {
            checkboxGroup.innerHTML += `
                <label class="checkbox-item">
                    <input type="checkbox" name="exp-strategy" value="${escapeHtml(s.name)}">
                    ${escapeHtml(s.name)}
                </label>
            `;
        }
    } catch (e) {
        console.error(e);
    }

    openModal('experiment-modal');
}

function resetExperimentInputs() {
    const container = document.getElementById('exp-inputs-container');
    container.innerHTML = `
        <div class="exp-input-row" style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
            <input type="file" class="exp-file-input" style="flex:1;">
        </div>
    `;
    document.getElementById('exp-advanced-input').checked = false;
    document.getElementById('exp-json-input').style.display = 'none';
    document.getElementById('exp-input-dataset').value = '[]';
    container.style.display = '';
}

function addExperimentInput() {
    const container = document.getElementById('exp-inputs-container');
    const row = document.createElement('div');
    row.className = 'exp-input-row';
    row.style.cssText = 'display:flex; gap:8px; align-items:center; margin-bottom:8px;';
    row.innerHTML = `
        <input type="file" class="exp-file-input" style="flex:1;">
        <button type="button" class="btn btn-danger btn-sm" onclick="removeExperimentInput(this)" style="flex-shrink:0;">✕</button>
    `;
    container.appendChild(row);
}

function removeExperimentInput(btn) {
    const row = btn.parentElement;
    const container = document.getElementById('exp-inputs-container');
    if (container.querySelectorAll('.exp-input-row').length > 1) {
        row.remove();
    } else {
        showToast('至少保留一个输入', 'error');
    }
}

function toggleExpInputMode() {
    const advanced = document.getElementById('exp-advanced-input').checked;
    document.getElementById('exp-json-input').style.display = advanced ? '' : 'none';
    document.getElementById('exp-inputs-container').style.display = advanced ? 'none' : '';
}

async function createExperiment() {
    const name = document.getElementById('exp-name').value.trim();
    const app_name = document.getElementById('exp-app').value.trim();

    if (!name || !app_name) {
        showToast('请填写实验名称并选择应用', 'error');
        return;
    }

    const selectedStrategies = Array.from(document.querySelectorAll('input[name="exp-strategy"]:checked'))
        .map(cb => cb.value);
    if (selectedStrategies.length === 0) {
        showToast('请至少选择一个策略', 'error');
        return;
    }

    const rounds = parseInt(document.getElementById('exp-rounds').value) || 1;
    const max_retries = parseInt(document.getElementById('exp-max-retries').value) || 0;

    // 收集输入数据
    let input_dataset;
    const advancedMode = document.getElementById('exp-advanced-input').checked;

    if (advancedMode) {
        // 高级模式：从 textarea 解析 JSON
        try {
            input_dataset = JSON.parse(document.getElementById('exp-input-dataset').value || '[]');
        } catch (e) {
            showToast('输入数据JSON格式无效', 'error');
            return;
        }
        if (!Array.isArray(input_dataset)) input_dataset = [input_dataset];
    } else {
        // 文件上传模式：遍历所有文件输入框，逐个上传
        const fileInputs = document.querySelectorAll('.exp-file-input');
        input_dataset = [];
        for (const input of fileInputs) {
            if (input.files && input.files[0]) {
                try {
                    const formData = new FormData();
                    formData.append('file', input.files[0]);
                    const result = await apiUpload('/files/upload', formData);
                    input_dataset.push({ file_id: result.file_id });
                } catch (e) {
                    showToast('文件上传失败: ' + e.message, 'error');
                    return;
                }
            }
        }
    }

    if (input_dataset.length === 0) {
        showToast('请至少提供一个输入文件', 'error');
        return;
    }

    const body = {
        name, app_name,
        strategy_group: selectedStrategies,
        input_dataset, rounds, max_retries,
        result_method: 'db'
    };

    try {
        const result = await apiCall('/experiments', 'POST', body);
        showToast(`实验已创建，共 ${result.task_count} 个任务`);
        closeModal('experiment-modal');
        loadExperiments();
    } catch (e) {
        showToast('创建实验失败: ' + e.message, 'error');
    }
}

async function deleteExperiment(expId) {
    if (!confirmDelete(expId)) return;
    try {
        await apiCall(`/experiments/${expId}`, 'DELETE');
        showToast('实验已删除');
        loadExperiments();
    } catch (e) {
        showToast('删除实验失败: ' + e.message, 'error');
    }
}

async function showExperimentReport(expId) {
    try {
        const report = await apiCall(`/experiments/${expId}/report`, 'GET');
        const content = document.getElementById('experiment-report-content');

        let html = `
            <div class="report-grid">
                <div class="report-stat">
                    <div class="report-stat-value">${report.total_tasks}</div>
                    <div class="report-stat-label">总任务数</div>
                </div>
                <div class="report-stat">
                    <div class="report-stat-value" style="color:#10b981;">${report.completed_tasks}</div>
                    <div class="report-stat-label">已完成</div>
                </div>
                <div class="report-stat">
                    <div class="report-stat-value" style="color:#ef4444;">${report.failed_tasks}</div>
                    <div class="report-stat-label">失败</div>
                </div>
                <div class="report-stat">
                    <div class="report-stat-value">${report.avg_execution_time_ms ? report.avg_execution_time_ms.toFixed(1) + 'ms' : '-'}</div>
                    <div class="report-stat-label">平均执行时间</div>
                </div>
            </div>
        `;

        // 策略对比
        if (report.strategy_breakdown && report.strategy_breakdown.length > 0) {
            html += `
                <div class="report-section">
                    <div class="report-section-title">策略对比</div>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>策略</th>
                                    <th>总数</th>
                                    <th>完成</th>
                                    <th>失败</th>
                                    <th>成功率</th>
                                    <th>平均耗时</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${report.strategy_breakdown.map(s => `
                                    <tr>
                                        <td>${escapeHtml(s.strategy_name)}</td>
                                        <td>${s.total}</td>
                                        <td>${s.completed}</td>
                                        <td>${s.failed}</td>
                                        <td>${(s.success_rate * 100).toFixed(1)}%</td>
                                        <td>${s.avg_execution_time_ms ? s.avg_execution_time_ms.toFixed(1) + 'ms' : '-'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        // 阶段耗时
        if (report.stage_breakdown && report.stage_breakdown.length > 0) {
            html += `
                <div class="report-section">
                    <div class="report-section-title">阶段耗时统计</div>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>阶段</th>
                                    <th>执行次数</th>
                                    <th>总耗时</th>
                                    <th>平均耗时</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${report.stage_breakdown.map(s => `
                                    <tr>
                                        <td>${escapeHtml(s.stage_name)}</td>
                                        <td>${s.execution_count}</td>
                                        <td>${s.total_time_ms ? s.total_time_ms.toFixed(1) + 'ms' : '-'}</td>
                                        <td>${s.avg_execution_time_ms ? s.avg_execution_time_ms.toFixed(1) + 'ms' : '-'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        // 层级分布
        if (report.tier_breakdown && report.tier_breakdown.length > 0) {
            html += `
                <div class="report-section">
                    <div class="report-section-title">层级分布</div>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>层级</th>
                                    <th>执行次数</th>
                                    <th>总耗时</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${report.tier_breakdown.map(t => `
                                    <tr>
                                        <td>${escapeHtml(t.tier)}</td>
                                        <td>${t.execution_count}</td>
                                        <td>${t.total_time_ms ? t.total_time_ms.toFixed(1) + 'ms' : '-'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        // 错误列表
        if (report.errors && report.errors.length > 0) {
            html += `
                <div class="report-section">
                    <div class="report-section-title" style="color:#ef4444;">错误列表 (${report.errors.length})</div>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>任务ID</th>
                                    <th>阶段</th>
                                    <th>错误信息</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${report.errors.slice(0, 20).map(e => `
                                    <tr>
                                        <td style="font-family:monospace; font-size:12px;">${escapeHtml(e.task_id)}</td>
                                        <td>${escapeHtml(e.stage)}</td>
                                        <td style="color:#ef4444; max-width:400px; word-break:break-all;">${escapeHtml(e.error)}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        // 导出按钮
        html += `
            <div style="margin-top:16px; display:flex; gap:12px; justify-content:flex-end;">
                <button class="btn btn-secondary" onclick="exportExperimentJSON('${expId}')">导出 JSON</button>
                <button class="btn btn-secondary" onclick="exportExperimentCSV('${expId}')">导出 CSV</button>
            </div>
        `;

        content.innerHTML = html;
        openModal('experiment-report-modal');

        // 保存报告数据供导出使用
        window._currentReport = report;
    } catch (e) {
        showToast('获取实验报告失败: ' + e.message, 'error');
    }
}

function exportExperimentJSON(expId) {
    if (!window._currentReport) {
        showToast('无报告数据可导出', 'error');
        return;
    }
    exportJSON(window._currentReport, `experiment_${expId}_report.json`);
}

function exportExperimentCSV(expId) {
    if (!window._currentReport) {
        showToast('无报告数据可导出', 'error');
        return;
    }
    const report = window._currentReport;

    // 构建 CSV: 每个任务一行
    const headers = ['task_id', 'strategy_name', 'status', 'retry_count', 'stage_name', 'node_id', 'node_tier', 'execution_time_ms', 'transfer_time_ms', 'cpu_percent', 'memory_mb', 'error_msg'];
    const rows = [];

    if (report.task_details) {
        for (const task of report.task_details) {
            if (task.trace && task.trace.length > 0) {
                for (const step of task.trace) {
                    rows.push([
                        task.task_id,
                        task.strategy_name,
                        task.status,
                        task.retry_count || 0,
                        step.stage_name || '',
                        step.node_id || '',
                        step.node_tier || '',
                        step.execution_time_ms || '',
                        step.transfer_time_ms || '',
                        step.cpu_percent || '',
                        step.memory_mb || '',
                        step.error_msg || ''
                    ]);
                }
            } else {
                rows.push([
                    task.task_id, task.strategy_name, task.status, task.retry_count || 0,
                    '', '', '', '', '', '', '', ''
                ]);
            }
        }
    }

    exportCSV(headers, rows, `experiment_${expId}_report.csv`);
}
