const API_BASE = '/api/v1';

async function apiCall(url, method = 'GET', body = null) {
    const options = {
        method,
        headers: { 'Content-Type': 'application/json' }
    };
    if (body) options.body = JSON.stringify(body);

    const response = await fetch(API_BASE + url, options);
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status}: ${text}`);
    }
    if (response.status === 204) return null;
    return response.json();
}

async function apiUpload(url, formData) {
    const response = await fetch(API_BASE + url, {
        method: 'POST',
        body: formData
    });
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status}: ${text}`);
    }
    return response.json();
}

function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast ${type} active`;
    setTimeout(() => toast.classList.remove('active'), 3000);
}

function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function formatBeijingTime(dateStr) {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    return date.toLocaleString('zh-CN', {
        timeZone: 'Asia/Shanghai',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

function formatBytes(bytes) {
    if (bytes == null || bytes === 0) return '-';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function statusBadge(status) {
    const map = {
        'completed': 'badge-success',
        'failed': 'badge-error',
        'running': 'badge-warning',
        'pending': 'badge-info',
        'completed_with_errors': 'badge-warning'
    };
    return `<span class="badge ${map[status] || 'badge-info'}">${status || 'pending'}</span>`;
}

function confirmDelete(itemName) {
    return confirm(`确定要删除 "${itemName}" 吗？此操作不可撤销。`);
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
