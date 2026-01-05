
// Registration Device Data (Placeholder - will be replaced by backend output)
const regDeviceData = [];

// Registration Device Table Logic
const regDeviceTableBody = document.getElementById('regDeviceTableBody');
if (regDeviceTableBody && regDeviceData.length > 0) {
    regDeviceTableBody.innerHTML = '';

    regDeviceData.forEach(row => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';

        tr.innerHTML = `
            <td style="padding: 1rem;">${row.month}</td>
            <td style="padding: 1rem; color: #10b981;">${row.totalReg.toLocaleString()}</td>
            <td style="padding: 1rem;">${row.boundDevice.toLocaleString()}</td>
            <td style="padding: 1rem; color: #fbbf24;">${row.boundDevicePct}%</td>
            <td style="padding: 1rem;">${row.ownerDevice.toLocaleString()}</td>
            <td style="padding: 1rem; color: #3b82f6;">${row.ownerDevicePct}%</td>
        `;
        regDeviceTableBody.appendChild(tr);
    });
}
