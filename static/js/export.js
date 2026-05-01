// ============= JSON/CSV 前端导出工具 =============

function exportJSON(data, filename) {
    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    downloadBlob(blob, filename);
}

function exportCSV(headers, rows, filename) {
    const csvContent = [
        headers.join(','),
        ...rows.map(row => row.map(cell => escapeCSV(cell)).join(','))
    ].join('\n');

    // 添加 BOM 以支持中文在 Excel 中正确显示
    const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8' });
    downloadBlob(blob, filename);
}

function escapeCSV(value) {
    if (value == null) return '';
    const str = String(value);
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    showToast(`已下载 ${filename}`);
}
