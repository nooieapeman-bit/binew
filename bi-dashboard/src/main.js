import Chart from 'chart.js/auto';
import ChartDataLabels from 'chartjs-plugin-datalabels';

// Register the plugin to all charts
Chart.register(ChartDataLabels);

const revenueData = [
    597354.56,
    568862.46,
    642631.3,
    611978.44,
    634867.87,
    627893.54,
    689907.21,
    852698.55,
    906161.08,
    869195.49,
    855107.85,
    927525.65
];

const trialData = [
    1477,
    1787,
    1325,
    1043,
    1119,
    1253,
    1764,
    1944,
    1872,
    1786,
    1903,
    2504
];

const validOrdersData = [
    16406,
    15374,
    16663,
    15873,
    16023,
    15600,
    16636,
    17388,
    16961,
    16809,
    16146,
    16750
];

const firstPeriodData = [
    2878,
    2652,
    2832,
    2245,
    2245,
    2405,
    3025,
    3201,
    2858,
    2698,
    2669,
    3275
];

const renewalData = validOrdersData.map((val, index) => val - firstPeriodData[index]);

const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            display: false
        },
        tooltip: {
            backgroundColor: 'rgba(15, 23, 42, 0.9)',
            titleColor: '#e2e8f0',
            bodyColor: '#cbd5e1',
            padding: 12,
            cornerRadius: 8,
            displayColors: false
        },
        datalabels: {
            align: 'end',
            anchor: 'end',
            color: '#94a3b8',
            font: {
                weight: 'bold',
                size: 11
            },
            formatter: (value) => value.toLocaleString()
        }
    },
    scales: {
        y: {
            beginAtZero: true,
            grid: {
                color: 'rgba(255, 255, 255, 0.05)'
            },
            ticks: {
                color: '#64748b'
            }
        },
        x: {
            grid: {
                display: false
            },
            ticks: {
                color: '#64748b'
            }
        }
    }
};

// 1. Revenue Chart
const revenueCtx = document.getElementById('revenueChart');
if (revenueCtx) {
    new Chart(revenueCtx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Revenue (CNY)',
                data: revenueData,
                backgroundColor: '#3b82f6',
                borderRadius: 6
            }]
        },
        options: {
            ...chartOptions,
            plugins: {
                ...chartOptions.plugins,
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.parsed.y !== null) {
                                label += new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'CNY' }).format(context.parsed.y);
                            }
                            return label;
                        }
                    }
                },
                datalabels: {
                    ...chartOptions.plugins.datalabels,
                    formatter: (value) => '¥' + (value / 1000).toFixed(0) + 'k'
                }
            }
        }
    });
}

// 2. Paid Orders Chart
const paidOrdersCtx = document.getElementById('paidOrdersChart');
if (paidOrdersCtx) {
    new Chart(paidOrdersCtx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Valid Paid Orders',
                data: validOrdersData,
                backgroundColor: '#10b981',
                borderRadius: 6
            }]
        },
        options: chartOptions
    });
}

// 3. Trial Orders Chart
const trialCtx = document.getElementById('trialChart');
if (trialCtx) {
    new Chart(trialCtx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Trial Orders',
                data: trialData,
                backgroundColor: '#fbbf24',
                borderRadius: 6
            }]
        },
        options: chartOptions
    });
}

// 4. First Period Orders Chart
const firstPeriodCtx = document.getElementById('firstPeriodChart');
if (firstPeriodCtx) {
    new Chart(firstPeriodCtx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'First Period Paid Orders',
                data: firstPeriodData,
                backgroundColor: '#8b5cf6',
                borderRadius: 6
            }]
        },
        options: chartOptions
    });
}

// 5. Renewal Orders Chart
const renewalCtx = document.getElementById('renewalChart');
if (renewalCtx) {
    new Chart(renewalCtx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Renewal Orders',
                data: renewalData,
                backgroundColor: '#06b6d4',
                borderRadius: 6
            }]
        },
        options: chartOptions
    });
}



// Cohort Analysis Data (Fixed Window with Plan Breakdown)
const cohortData = [
    {
        "month": "2025-01",
        "period": "2024-12-18 ~ 2025-01-17",
        "convertedTotal": 993,
        "convertedMonthly": 882,
        "convertedMonthlyPct": 88.82,
        "convertedYearly": 111,
        "convertedYearlyPct": 11.18,
        "trialTotal": 1529,
        "trialMonthly": 1402,
        "trialMonthlyPct": 91.69,
        "trialYearly": 127,
        "trialYearlyPct": 8.31,
        "rate": 64.94,
        "monthlyRate": 62.91,
        "yearlyRate": 87.4
    },
    {
        "month": "2025-02",
        "period": "2025-01-18 ~ 2025-02-14",
        "convertedTotal": 805,
        "convertedMonthly": 725,
        "convertedMonthlyPct": 90.06,
        "convertedYearly": 80,
        "convertedYearlyPct": 9.94,
        "trialTotal": 1343,
        "trialMonthly": 1239,
        "trialMonthlyPct": 92.26,
        "trialYearly": 104,
        "trialYearlyPct": 7.74,
        "rate": 59.94,
        "monthlyRate": 58.51,
        "yearlyRate": 76.92
    },
    {
        "month": "2025-03",
        "period": "2025-02-15 ~ 2025-03-17",
        "convertedTotal": 968,
        "convertedMonthly": 859,
        "convertedMonthlyPct": 88.74,
        "convertedYearly": 109,
        "convertedYearlyPct": 11.26,
        "trialTotal": 1902,
        "trialMonthly": 1735,
        "trialMonthlyPct": 91.22,
        "trialYearly": 167,
        "trialYearlyPct": 8.78,
        "rate": 50.89,
        "monthlyRate": 49.51,
        "yearlyRate": 65.27
    },
    {
        "month": "2025-04",
        "period": "2025-03-18 ~ 2025-04-16",
        "convertedTotal": 600,
        "convertedMonthly": 541,
        "convertedMonthlyPct": 90.17,
        "convertedYearly": 59,
        "convertedYearlyPct": 9.83,
        "trialTotal": 1108,
        "trialMonthly": 1020,
        "trialMonthlyPct": 92.06,
        "trialYearly": 88,
        "trialYearlyPct": 7.94,
        "rate": 54.15,
        "monthlyRate": 53.04,
        "yearlyRate": 67.05
    },
    {
        "month": "2025-05",
        "period": "2025-04-17 ~ 2025-05-17",
        "convertedTotal": 637,
        "convertedMonthly": 577,
        "convertedMonthlyPct": 90.58,
        "convertedYearly": 60,
        "convertedYearlyPct": 9.42,
        "trialTotal": 1058,
        "trialMonthly": 982,
        "trialMonthlyPct": 92.82,
        "trialYearly": 76,
        "trialYearlyPct": 7.18,
        "rate": 60.21,
        "monthlyRate": 58.76,
        "yearlyRate": 78.95
    },
    {
        "month": "2025-06",
        "period": "2025-05-18 ~ 2025-06-16",
        "convertedTotal": 670,
        "convertedMonthly": 602,
        "convertedMonthlyPct": 89.85,
        "convertedYearly": 68,
        "convertedYearlyPct": 10.15,
        "trialTotal": 1168,
        "trialMonthly": 1069,
        "trialMonthlyPct": 91.52,
        "trialYearly": 99,
        "trialYearlyPct": 8.48,
        "rate": 57.36,
        "monthlyRate": 56.31,
        "yearlyRate": 68.69
    },
    {
        "month": "2025-07",
        "period": "2025-06-17 ~ 2025-07-17",
        "convertedTotal": 944,
        "convertedMonthly": 875,
        "convertedMonthlyPct": 92.69,
        "convertedYearly": 69,
        "convertedYearlyPct": 7.31,
        "trialTotal": 1497,
        "trialMonthly": 1392,
        "trialMonthlyPct": 92.99,
        "trialYearly": 105,
        "trialYearlyPct": 7.01,
        "rate": 63.06,
        "monthlyRate": 62.86,
        "yearlyRate": 65.71
    },
    {
        "month": "2025-08",
        "period": "2025-07-18 ~ 2025-08-17",
        "convertedTotal": 1106,
        "convertedMonthly": 727,
        "convertedMonthlyPct": 65.73,
        "convertedYearly": 379,
        "convertedYearlyPct": 34.27,
        "trialTotal": 1976,
        "trialMonthly": 1282,
        "trialMonthlyPct": 64.88,
        "trialYearly": 694,
        "trialYearlyPct": 35.12,
        "rate": 55.97,
        "monthlyRate": 56.71,
        "yearlyRate": 54.61
    },
    {
        "month": "2025-09",
        "period": "2025-08-18 ~ 2025-09-16",
        "convertedTotal": 948,
        "convertedMonthly": 508,
        "convertedMonthlyPct": 53.59,
        "convertedYearly": 440,
        "convertedYearlyPct": 46.41,
        "trialTotal": 1874,
        "trialMonthly": 926,
        "trialMonthlyPct": 49.41,
        "trialYearly": 948,
        "trialYearlyPct": 50.59,
        "rate": 50.59,
        "monthlyRate": 54.86,
        "yearlyRate": 46.41
    },
    {
        "month": "2025-10",
        "period": "2025-09-17 ~ 2025-10-17",
        "convertedTotal": 823,
        "convertedMonthly": 485,
        "convertedMonthlyPct": 58.93,
        "convertedYearly": 338,
        "convertedYearlyPct": 41.07,
        "trialTotal": 1846,
        "trialMonthly": 926,
        "trialMonthlyPct": 50.16,
        "trialYearly": 920,
        "trialYearlyPct": 49.84,
        "rate": 44.58,
        "monthlyRate": 52.38,
        "yearlyRate": 36.74
    },
    {
        "month": "2025-11",
        "period": "2025-10-18 ~ 2025-11-16",
        "convertedTotal": 775,
        "convertedMonthly": 415,
        "convertedMonthlyPct": 53.55,
        "convertedYearly": 360,
        "convertedYearlyPct": 46.45,
        "trialTotal": 1831,
        "trialMonthly": 877,
        "trialMonthlyPct": 47.9,
        "trialYearly": 954,
        "trialYearlyPct": 52.1,
        "rate": 42.33,
        "monthlyRate": 47.32,
        "yearlyRate": 37.74
    },
    {
        "month": "2025-12",
        "period": "2025-11-17 ~ 2025-12-17",
        "convertedTotal": 963,
        "convertedMonthly": 555,
        "convertedMonthlyPct": 57.63,
        "convertedYearly": 408,
        "convertedYearlyPct": 42.37,
        "trialTotal": 2199,
        "trialMonthly": 1113,
        "trialMonthlyPct": 50.61,
        "trialYearly": 1086,
        "trialYearlyPct": 49.39,
        "rate": 43.79,
        "monthlyRate": 49.87,
        "yearlyRate": 37.57
    }
];

// Dynamically calculated conversionData from firstPeriodData and cohortData
const conversionData = firstPeriodData.map((total, index) => {
    const cohort = cohortData[index];
    const trial = cohort ? cohort.convertedTotal : 0;
    const direct = total - trial;
    const trialPct = total > 0 ? (trial / total * 100).toFixed(2) : 0;
    const directPct = total > 0 ? (direct / total * 100).toFixed(2) : 0;

    return {
        month: cohort ? cohort.month : months[index],
        total: total,
        trial: trial,
        trialPct: trialPct,
        direct: direct,
        directPct: directPct
    };
});

// Append Total Row
const totalTotal = conversionData.reduce((sum, item) => sum + item.total, 0);
const totalTrial = conversionData.reduce((sum, item) => sum + item.trial, 0);
const totalDirect = conversionData.reduce((sum, item) => sum + item.direct, 0);

conversionData.push({
    month: 'Total',
    total: totalTotal,
    trial: totalTrial,
    trialPct: (totalTotal > 0 ? (totalTrial / totalTotal * 100).toFixed(2) : "0.00"),
    direct: totalDirect,
    directPct: (totalTotal > 0 ? (totalDirect / totalTotal * 100).toFixed(2) : "0.00")
});

const lagData = [
    {
        "month": "2025-01",
        "period": "2024-12-18 ~ 2025-01-17",
        "totalTrials": 1513,
        "bins": {
            "Same Day": {
                "count": 437,
                "pct": 28.88
            },
            "1-3 Days": {
                "count": 167,
                "pct": 11.04
            },
            "4-7 Days": {
                "count": 70,
                "pct": 4.63
            },
            "8-14 Days": {
                "count": 74,
                "pct": 4.89
            },
            "15-30 Days": {
                "count": 89,
                "pct": 5.88
            },
            "30-60 Days": {
                "count": 79,
                "pct": 5.22
            },
            "60-90 Days": {
                "count": 62,
                "pct": 4.1
            },
            "> 90 Days": {
                "count": 535,
                "pct": 35.36
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-02",
        "period": "2025-01-18 ~ 2025-02-14",
        "totalTrials": 1334,
        "bins": {
            "Same Day": {
                "count": 386,
                "pct": 28.94
            },
            "1-3 Days": {
                "count": 163,
                "pct": 12.22
            },
            "4-7 Days": {
                "count": 75,
                "pct": 5.62
            },
            "8-14 Days": {
                "count": 54,
                "pct": 4.05
            },
            "15-30 Days": {
                "count": 79,
                "pct": 5.92
            },
            "30-60 Days": {
                "count": 75,
                "pct": 5.62
            },
            "60-90 Days": {
                "count": 41,
                "pct": 3.07
            },
            "> 90 Days": {
                "count": 461,
                "pct": 34.56
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-03",
        "period": "2025-02-15 ~ 2025-03-17",
        "totalTrials": 1824,
        "bins": {
            "Same Day": {
                "count": 472,
                "pct": 25.88
            },
            "1-3 Days": {
                "count": 187,
                "pct": 10.25
            },
            "4-7 Days": {
                "count": 84,
                "pct": 4.61
            },
            "8-14 Days": {
                "count": 105,
                "pct": 5.76
            },
            "15-30 Days": {
                "count": 101,
                "pct": 5.54
            },
            "30-60 Days": {
                "count": 102,
                "pct": 5.59
            },
            "60-90 Days": {
                "count": 70,
                "pct": 3.84
            },
            "> 90 Days": {
                "count": 703,
                "pct": 38.54
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-04",
        "period": "2025-03-18 ~ 2025-04-16",
        "totalTrials": 1066,
        "bins": {
            "Same Day": {
                "count": 271,
                "pct": 25.42
            },
            "1-3 Days": {
                "count": 134,
                "pct": 12.57
            },
            "4-7 Days": {
                "count": 48,
                "pct": 4.5
            },
            "8-14 Days": {
                "count": 42,
                "pct": 3.94
            },
            "15-30 Days": {
                "count": 72,
                "pct": 6.75
            },
            "30-60 Days": {
                "count": 59,
                "pct": 5.53
            },
            "60-90 Days": {
                "count": 37,
                "pct": 3.47
            },
            "> 90 Days": {
                "count": 403,
                "pct": 37.8
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-05",
        "period": "2025-04-17 ~ 2025-05-17",
        "totalTrials": 1019,
        "bins": {
            "Same Day": {
                "count": 267,
                "pct": 26.2
            },
            "1-3 Days": {
                "count": 103,
                "pct": 10.11
            },
            "4-7 Days": {
                "count": 41,
                "pct": 4.02
            },
            "8-14 Days": {
                "count": 45,
                "pct": 4.42
            },
            "15-30 Days": {
                "count": 50,
                "pct": 4.91
            },
            "30-60 Days": {
                "count": 58,
                "pct": 5.69
            },
            "60-90 Days": {
                "count": 34,
                "pct": 3.34
            },
            "> 90 Days": {
                "count": 421,
                "pct": 41.32
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-06",
        "period": "2025-05-18 ~ 2025-06-16",
        "totalTrials": 1160,
        "bins": {
            "Same Day": {
                "count": 342,
                "pct": 29.48
            },
            "1-3 Days": {
                "count": 120,
                "pct": 10.34
            },
            "4-7 Days": {
                "count": 50,
                "pct": 4.31
            },
            "8-14 Days": {
                "count": 43,
                "pct": 3.71
            },
            "15-30 Days": {
                "count": 47,
                "pct": 4.05
            },
            "30-60 Days": {
                "count": 58,
                "pct": 5.0
            },
            "60-90 Days": {
                "count": 47,
                "pct": 4.05
            },
            "> 90 Days": {
                "count": 453,
                "pct": 39.05
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-07",
        "period": "2025-06-17 ~ 2025-07-17",
        "totalTrials": 1446,
        "bins": {
            "Same Day": {
                "count": 464,
                "pct": 32.09
            },
            "1-3 Days": {
                "count": 172,
                "pct": 11.89
            },
            "4-7 Days": {
                "count": 77,
                "pct": 5.33
            },
            "8-14 Days": {
                "count": 57,
                "pct": 3.94
            },
            "15-30 Days": {
                "count": 60,
                "pct": 4.15
            },
            "30-60 Days": {
                "count": 56,
                "pct": 3.87
            },
            "60-90 Days": {
                "count": 33,
                "pct": 2.28
            },
            "> 90 Days": {
                "count": 527,
                "pct": 36.45
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-08",
        "period": "2025-07-18 ~ 2025-08-17",
        "totalTrials": 1947,
        "bins": {
            "Same Day": {
                "count": 649,
                "pct": 33.33
            },
            "1-3 Days": {
                "count": 218,
                "pct": 11.2
            },
            "4-7 Days": {
                "count": 84,
                "pct": 4.31
            },
            "8-14 Days": {
                "count": 81,
                "pct": 4.16
            },
            "15-30 Days": {
                "count": 108,
                "pct": 5.55
            },
            "30-60 Days": {
                "count": 84,
                "pct": 4.31
            },
            "60-90 Days": {
                "count": 40,
                "pct": 2.05
            },
            "> 90 Days": {
                "count": 683,
                "pct": 35.08
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-09",
        "period": "2025-08-18 ~ 2025-09-16",
        "totalTrials": 1849,
        "bins": {
            "Same Day": {
                "count": 648,
                "pct": 35.05
            },
            "1-3 Days": {
                "count": 225,
                "pct": 12.17
            },
            "4-7 Days": {
                "count": 94,
                "pct": 5.08
            },
            "8-14 Days": {
                "count": 67,
                "pct": 3.62
            },
            "15-30 Days": {
                "count": 96,
                "pct": 5.19
            },
            "30-60 Days": {
                "count": 91,
                "pct": 4.92
            },
            "60-90 Days": {
                "count": 52,
                "pct": 2.81
            },
            "> 90 Days": {
                "count": 576,
                "pct": 31.15
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-10",
        "period": "2025-09-17 ~ 2025-10-17",
        "totalTrials": 1829,
        "bins": {
            "Same Day": {
                "count": 683,
                "pct": 37.34
            },
            "1-3 Days": {
                "count": 260,
                "pct": 14.22
            },
            "4-7 Days": {
                "count": 100,
                "pct": 5.47
            },
            "8-14 Days": {
                "count": 85,
                "pct": 4.65
            },
            "15-30 Days": {
                "count": 94,
                "pct": 5.14
            },
            "30-60 Days": {
                "count": 64,
                "pct": 3.5
            },
            "60-90 Days": {
                "count": 41,
                "pct": 2.24
            },
            "> 90 Days": {
                "count": 502,
                "pct": 27.45
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-11",
        "period": "2025-10-18 ~ 2025-11-16",
        "totalTrials": 1782,
        "bins": {
            "Same Day": {
                "count": 691,
                "pct": 38.78
            },
            "1-3 Days": {
                "count": 218,
                "pct": 12.23
            },
            "4-7 Days": {
                "count": 84,
                "pct": 4.71
            },
            "8-14 Days": {
                "count": 90,
                "pct": 5.05
            },
            "15-30 Days": {
                "count": 113,
                "pct": 6.34
            },
            "30-60 Days": {
                "count": 83,
                "pct": 4.66
            },
            "60-90 Days": {
                "count": 45,
                "pct": 2.53
            },
            "> 90 Days": {
                "count": 458,
                "pct": 25.7
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    },
    {
        "month": "2025-12",
        "period": "2025-11-17 ~ 2025-12-17",
        "totalTrials": 2163,
        "bins": {
            "Same Day": {
                "count": 964,
                "pct": 44.57
            },
            "1-3 Days": {
                "count": 327,
                "pct": 15.12
            },
            "4-7 Days": {
                "count": 120,
                "pct": 5.55
            },
            "8-14 Days": {
                "count": 85,
                "pct": 3.93
            },
            "15-30 Days": {
                "count": 106,
                "pct": 4.9
            },
            "30-60 Days": {
                "count": 87,
                "pct": 4.02
            },
            "60-90 Days": {
                "count": 57,
                "pct": 2.64
            },
            "> 90 Days": {
                "count": 417,
                "pct": 19.28
            },
            "Unmatched": {
                "count": 0,
                "pct": 0.0
            }
        }
    }
];

// Matrix Table Logic (Main Performance Matrix)
const tableBody = document.getElementById('conversionTableBody');
if (tableBody) {
    // Clear existing rows
    tableBody.innerHTML = '';

    let grandTotals = {
        trial: 0,
        valid: 0,
        first: 0,
        fromTrial: 0,
        fromDirect: 0,
        renewal: 0
    };

    // Iterate through first 12 entries (months)
    for (let i = 0; i < 12; i++) {
        // Stick to validOrdersData/trialData length which is 12.
        const rowData = conversionData[i];
        const monthLabel = rowData.month; // e.g. '2025-01'

        // Metrics
        const trials = trialData[i];
        const valid = validOrdersData[i];
        const first = firstPeriodData[i];
        const renewal = renewalData[i];

        // Breakdown from conversionData
        const breakdown = conversionData[i];
        const fromTrial = breakdown ? breakdown.trial : 0;
        const fromDirect = breakdown ? breakdown.direct : 0;

        // Percentages
        const pctTrial = first > 0 ? (fromTrial / first * 100).toFixed(2) + '%' : '0.00%';
        const pctDirect = first > 0 ? (fromDirect / first * 100).toFixed(2) + '%' : '0.00%';

        // Accumulate Totals
        grandTotals.trial += trials;
        grandTotals.valid += valid;
        grandTotals.first += first;
        grandTotals.fromTrial += fromTrial;
        grandTotals.fromDirect += fromDirect;
        grandTotals.renewal += renewal;

        // Render Row
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';
        tr.innerHTML = `
            <td style="padding: 1rem;">${monthLabel}</td>
            <td style="padding: 1rem; color: #fbbf24;">${trials.toLocaleString()}</td>
            <td style="padding: 1rem; color: #10b981;">${valid.toLocaleString()}</td>
            <td style="padding: 1rem; color: #8b5cf6;">${first.toLocaleString()}</td>
            <td style="padding: 1rem;">${fromTrial.toLocaleString()}</td>
            <td style="padding: 1rem; color: #fbbf24;">${pctTrial}</td>
            <td style="padding: 1rem;">${fromDirect.toLocaleString()}</td>
            <td style="padding: 1rem; color: #3b82f6;">${pctDirect}</td>
            <td style="padding: 1rem; color: #06b6d4;">${renewal.toLocaleString()}</td>
        `;
        tableBody.appendChild(tr);
    }

    // Render Total Row
    const grandPctTrial = grandTotals.first > 0 ? (grandTotals.fromTrial / grandTotals.first * 100).toFixed(2) + '%' : '0.00%';
    const grandPctDirect = grandTotals.first > 0 ? (grandTotals.fromDirect / grandTotals.first * 100).toFixed(2) + '%' : '0.00%';

    const totalTr = document.createElement('tr');
    totalTr.style.fontWeight = 'bold';
    totalTr.style.backgroundColor = 'rgba(255,255,255,0.05)';
    totalTr.innerHTML = `
        <td style="padding: 1rem;">Total</td>
        <td style="padding: 1rem; color: #fbbf24;">${grandTotals.trial.toLocaleString()}</td>
        <td style="padding: 1rem; color: #10b981;">${grandTotals.valid.toLocaleString()}</td>
        <td style="padding: 1rem; color: #8b5cf6;">${grandTotals.first.toLocaleString()}</td>
        <td style="padding: 1rem;">${grandTotals.fromTrial.toLocaleString()}</td>
        <td style="padding: 1rem; color: #fbbf24;">${grandPctTrial}</td>
        <td style="padding: 1rem;">${grandTotals.fromDirect.toLocaleString()}</td>
        <td style="padding: 1rem; color: #3b82f6;">${grandPctDirect}</td>
        <td style="padding: 1rem; color: #06b6d4;">${grandTotals.renewal.toLocaleString()}</td>
    `;
    tableBody.appendChild(totalTr);
}

// Cohort Analysis Table Logic (New Table)
const cohortTableBody = document.getElementById('cohortTableBody');
if (cohortTableBody) {
    cohortTableBody.innerHTML = '';

    cohortData.forEach(row => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';

        tr.innerHTML = `
            <td style="padding: 1rem; font-size: 0.8rem; color: #94a3b8;">${row.period}</td>
            <td style="padding: 1rem;">${row.convertedTotal.toLocaleString()}</td>
            <td style="padding: 1rem; color: #a78bfa;">${row.convertedMonthly.toLocaleString()} <span style="font-size:0.8em; opacity:0.7;">(${row.convertedMonthlyPct}%)</span></td>
            <td style="padding: 1rem; color: #fbbf24;">${row.convertedYearly.toLocaleString()} <span style="font-size:0.8em; opacity:0.7;">(${row.convertedYearlyPct}%)</span></td>
            <td style="padding: 1rem;">${row.trialTotal.toLocaleString()}</td>
            <td style="padding: 1rem; color: #a78bfa;">${row.trialMonthly.toLocaleString()} <span style="font-size:0.8em; opacity:0.7;">(${row.trialMonthlyPct}%)</span></td>
            <td style="padding: 1rem; color: #fbbf24;">${row.trialYearly.toLocaleString()} <span style="font-size:0.8em; opacity:0.7;">(${row.trialYearlyPct}%)</span></td>
            <td style="padding: 1rem; color: #f472b6;">${row.monthlyRate.toFixed(2)}%</td>
            <td style="padding: 1rem; color: #f472b6;">${row.yearlyRate.toFixed(2)}%</td>
            <td style="padding: 1rem; font-weight: bold; color: #f472b6;">${row.rate.toFixed(2)}%</td>
        `;
        cohortTableBody.appendChild(tr);
    });
}

// Registration Lag Analysis Data


// Lag Analysis Table Logic
const lagTableBody = document.getElementById('lagTableBody');
if (lagTableBody) {
    lagTableBody.innerHTML = '';

    lagData.forEach(row => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';

        // Helper to format cell: "Count (Pct%)"
        const fmt = (binName) => {
            const b = row.bins[binName];
            return `${b.count.toLocaleString()} <span style="font-size:0.8em; opacity:0.7;">(${b.pct}%)</span>`;
        };

        tr.innerHTML = `
            <td style="padding: 1rem; font-size: 0.8rem; color: #94a3b8;">${row.period}</td>
            <td style="padding: 1rem; color: #fbbf24;">${row.totalTrials.toLocaleString()}</td>
            <td style="padding: 1rem;">${fmt('Same Day')}</td>
            <td style="padding: 1rem;">${fmt('1-3 Days')}</td>
            <td style="padding: 1rem;">${fmt('4-7 Days')}</td>
            <td style="padding: 1rem;">${fmt('8-14 Days')}</td>
            <td style="padding: 1rem;">${fmt('15-30 Days')}</td>
            <td style="padding: 1rem;">${fmt('30-60 Days')}</td>
            <td style="padding: 1rem;">${fmt('60-90 Days')}</td>
            <td style="padding: 1rem;">${fmt('> 90 Days')}</td>
        `;
        lagTableBody.appendChild(tr);
    });
}


// Registration Device Data (Placeholder - will be replaced by backend output)
const regDeviceData = [
    {
        "month": "2025-01",
        "totalReg": 11451,
        "boundDevice": 8872,
        "boundDevicePct": 77.48,
        "ownerDevice": 7781,
        "ownerDevicePct": 67.95,
        "ownerTrials": 1147,
        "ownerTrialsPct": 14.74,
        "ownerTrials30d": 863,
        "ownerTrials30dPct": 11.09
    },
    {
        "month": "2025-02",
        "totalReg": 10478,
        "boundDevice": 8191,
        "boundDevicePct": 78.17,
        "ownerDevice": 7180,
        "ownerDevicePct": 68.52,
        "ownerTrials": 1086,
        "ownerTrialsPct": 15.13,
        "ownerTrials30d": 860,
        "ownerTrials30dPct": 11.98
    },
    {
        "month": "2025-03",
        "totalReg": 10430,
        "boundDevice": 8224,
        "boundDevicePct": 78.85,
        "ownerDevice": 7193,
        "ownerDevicePct": 68.96,
        "ownerTrials": 953,
        "ownerTrialsPct": 13.25,
        "ownerTrials30d": 679,
        "ownerTrials30dPct": 9.44
    },
    {
        "month": "2025-04",
        "totalReg": 9014,
        "boundDevice": 7047,
        "boundDevicePct": 78.18,
        "ownerDevice": 5855,
        "ownerDevicePct": 64.95,
        "ownerTrials": 661,
        "ownerTrialsPct": 11.29,
        "ownerTrials30d": 484,
        "ownerTrials30dPct": 8.27
    },
    {
        "month": "2025-05",
        "totalReg": 9223,
        "boundDevice": 7395,
        "boundDevicePct": 80.18,
        "ownerDevice": 6071,
        "ownerDevicePct": 65.82,
        "ownerTrials": 724,
        "ownerTrialsPct": 11.93,
        "ownerTrials30d": 542,
        "ownerTrials30dPct": 8.93
    },
    {
        "month": "2025-06",
        "totalReg": 9833,
        "boundDevice": 7976,
        "boundDevicePct": 81.11,
        "ownerDevice": 6538,
        "ownerDevicePct": 66.49,
        "ownerTrials": 890,
        "ownerTrialsPct": 13.61,
        "ownerTrials30d": 687,
        "ownerTrials30dPct": 10.51
    },
    {
        "month": "2025-07",
        "totalReg": 12602,
        "boundDevice": 10651,
        "boundDevicePct": 84.52,
        "ownerDevice": 8709,
        "ownerDevicePct": 69.11,
        "ownerTrials": 1239,
        "ownerTrialsPct": 14.23,
        "ownerTrials30d": 1046,
        "ownerTrials30dPct": 12.01
    },
    {
        "month": "2025-08",
        "totalReg": 12409,
        "boundDevice": 10408,
        "boundDevicePct": 83.87,
        "ownerDevice": 8651,
        "ownerDevicePct": 69.72,
        "ownerTrials": 1187,
        "ownerTrialsPct": 13.72,
        "ownerTrials30d": 1045,
        "ownerTrials30dPct": 12.08
    },
    {
        "month": "2025-09",
        "totalReg": 13984,
        "boundDevice": 12074,
        "boundDevicePct": 86.34,
        "ownerDevice": 10114,
        "ownerDevicePct": 72.33,
        "ownerTrials": 1356,
        "ownerTrialsPct": 13.41,
        "ownerTrials30d": 1205,
        "ownerTrials30dPct": 11.91
    },
    {
        "month": "2025-10",
        "totalReg": 15992,
        "boundDevice": 13788,
        "boundDevicePct": 86.22,
        "ownerDevice": 11546,
        "ownerDevicePct": 72.2,
        "ownerTrials": 1278,
        "ownerTrialsPct": 11.07,
        "ownerTrials30d": 1150,
        "ownerTrials30dPct": 9.96
    },
    {
        "month": "2025-11",
        "totalReg": 17330,
        "boundDevice": 14673,
        "boundDevicePct": 84.67,
        "ownerDevice": 12096,
        "ownerDevicePct": 69.8,
        "ownerTrials": 1329,
        "ownerTrialsPct": 10.99,
        "ownerTrials30d": 1261,
        "ownerTrials30dPct": 10.42
    },
    {
        "month": "2025-12",
        "totalReg": 24997,
        "boundDevice": 21746,
        "boundDevicePct": 86.99,
        "ownerDevice": 18122,
        "ownerDevicePct": 72.5,
        "ownerTrials": 1800,
        "ownerTrialsPct": 9.93,
        "ownerTrials30d": 1796,
        "ownerTrials30dPct": 9.91
    }
];

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
            <td style="padding: 1rem;">${(row.ownerTrials || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #a855f7;">${(row.ownerTrialsPct || 0)}%</td>
            <td style="padding: 1rem;">${(row.ownerTrials30d || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #f472b6;">${(row.ownerTrials30dPct || 0)}%</td>
        `;
        regDeviceTableBody.appendChild(tr);
    });
}

// 6. Registration Chart
const registrationCtx = document.getElementById('registrationChart');
if (registrationCtx && typeof regDeviceData !== 'undefined') {
    const regLabels = regDeviceData.map(d => d.month);
    const regValues = regDeviceData.map(d => d.totalReg);

    new Chart(registrationCtx, {
        type: 'bar',
        data: {
            labels: regLabels,
            datasets: [{
                label: 'Registered Users',
                data: regValues,
                backgroundColor: '#8b5cf6', // Purple theme
                borderRadius: 6
            }]
        },
        options: {
            ...chartOptions,
            plugins: {
                ...chartOptions.plugins,
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return 'Registered: ' + context.parsed.y.toLocaleString();
                        }
                    }
                }
            }
        }
    });
}

// 7. Business Performance Matrix Logic
const businessMatrixData = [
    {
        "month": "2025-01",
        "totalDirectBuyers": 13072,
        "lag1m": 651,
        "lag1mPct": 4.98,
        "lag2to4m": 1971,
        "lag2to4mPct": 15.08,
        "lag5to7m": 2329,
        "lag5to7mPct": 17.82,
        "lag8to12m": 3207,
        "lag8to12mPct": 24.53,
        "lagGt12m": 4914,
        "lagGt12mPct": 37.59
    },
    {
        "month": "2025-02",
        "totalDirectBuyers": 12557,
        "lag1m": 590,
        "lag1mPct": 4.7,
        "lag2to4m": 1592,
        "lag2to4mPct": 12.68,
        "lag5to7m": 2026,
        "lag5to7mPct": 16.13,
        "lag8to12m": 3205,
        "lag8to12mPct": 25.52,
        "lagGt12m": 5144,
        "lagGt12mPct": 40.97
    },
    {
        "month": "2025-03",
        "totalDirectBuyers": 13140,
        "lag1m": 629,
        "lag1mPct": 4.79,
        "lag2to4m": 1471,
        "lag2to4mPct": 11.19,
        "lag5to7m": 1849,
        "lag5to7mPct": 14.07,
        "lag8to12m": 3384,
        "lag8to12mPct": 25.75,
        "lagGt12m": 5807,
        "lagGt12mPct": 44.19
    },
    {
        "month": "2025-04",
        "totalDirectBuyers": 12688,
        "lag1m": 375,
        "lag1mPct": 2.96,
        "lag2to4m": 1298,
        "lag2to4mPct": 10.23,
        "lag5to7m": 1606,
        "lag5to7mPct": 12.66,
        "lag8to12m": 3257,
        "lag8to12mPct": 25.67,
        "lagGt12m": 6152,
        "lagGt12mPct": 48.49
    },
    {
        "month": "2025-05",
        "totalDirectBuyers": 12600,
        "lag1m": 386,
        "lag1mPct": 3.06,
        "lag2to4m": 1098,
        "lag2to4mPct": 8.71,
        "lag5to7m": 1384,
        "lag5to7mPct": 10.98,
        "lag8to12m": 3090,
        "lag8to12mPct": 24.52,
        "lagGt12m": 6642,
        "lagGt12mPct": 52.71
    },
    {
        "month": "2025-06",
        "totalDirectBuyers": 12521,
        "lag1m": 442,
        "lag1mPct": 3.53,
        "lag2to4m": 965,
        "lag2to4mPct": 7.71,
        "lag5to7m": 1162,
        "lag5to7mPct": 9.28,
        "lag8to12m": 2925,
        "lag8to12mPct": 23.36,
        "lagGt12m": 7027,
        "lagGt12mPct": 56.12
    },
    {
        "month": "2025-07",
        "totalDirectBuyers": 12994,
        "lag1m": 667,
        "lag1mPct": 5.13,
        "lag2to4m": 898,
        "lag2to4mPct": 6.91,
        "lag5to7m": 1103,
        "lag5to7mPct": 8.49,
        "lag8to12m": 2639,
        "lag8to12mPct": 20.31,
        "lagGt12m": 7687,
        "lagGt12mPct": 59.16
    },
    {
        "month": "2025-08",
        "totalDirectBuyers": 13649,
        "lag1m": 763,
        "lag1mPct": 5.59,
        "lag2to4m": 1148,
        "lag2to4mPct": 8.41,
        "lag5to7m": 950,
        "lag5to7mPct": 6.96,
        "lag8to12m": 2352,
        "lag8to12mPct": 17.23,
        "lagGt12m": 8436,
        "lagGt12mPct": 61.81
    },
    {
        "month": "2025-09",
        "totalDirectBuyers": 13567,
        "lag1m": 746,
        "lag1mPct": 5.5,
        "lag2to4m": 1243,
        "lag2to4mPct": 9.16,
        "lag5to7m": 823,
        "lag5to7mPct": 6.07,
        "lag8to12m": 2064,
        "lag8to12mPct": 15.21,
        "lagGt12m": 8691,
        "lagGt12mPct": 64.06
    },
    {
        "month": "2025-10",
        "totalDirectBuyers": 13177,
        "lag1m": 733,
        "lag1mPct": 5.56,
        "lag2to4m": 1205,
        "lag2to4mPct": 9.14,
        "lag5to7m": 739,
        "lag5to7mPct": 5.61,
        "lag8to12m": 1751,
        "lag8to12mPct": 13.29,
        "lagGt12m": 8749,
        "lagGt12mPct": 66.4
    },
    {
        "month": "2025-11",
        "totalDirectBuyers": 12816,
        "lag1m": 714,
        "lag1mPct": 5.57,
        "lag2to4m": 1105,
        "lag2to4mPct": 8.62,
        "lag5to7m": 810,
        "lag5to7mPct": 6.32,
        "lag8to12m": 1447,
        "lag8to12mPct": 11.29,
        "lagGt12m": 8740,
        "lagGt12mPct": 68.2
    },
    {
        "month": "2025-12",
        "totalDirectBuyers": 12971,
        "lag1m": 970,
        "lag1mPct": 7.48,
        "lag2to4m": 1069,
        "lag2to4mPct": 8.24,
        "lag5to7m": 872,
        "lag5to7mPct": 6.72,
        "lag8to12m": 1259,
        "lag8to12mPct": 9.71,
        "lagGt12m": 8801,
        "lagGt12mPct": 67.85
    }
];

const businessMatrixTableBody = document.getElementById('businessMatrixTableBody');
if (businessMatrixTableBody && businessMatrixData.length > 0) {
    businessMatrixTableBody.innerHTML = '';
    businessMatrixData.forEach(row => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';

        const html = `
            <td style="padding: 1rem;">${row.month}</td>
            <td style="padding: 1rem; color: #10b981;">${row.totalDirectBuyers.toLocaleString()}</td>
            
            <td style="padding: 1rem;">${(row.lag1m || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #fbbf24;">${(row.lag1mPct || 0)}%</td>

            <td style="padding: 1rem;">${(row.lag2to4m || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #3b82f6;">${(row.lag2to4mPct || 0)}%</td>

            <td style="padding: 1rem;">${(row.lag5to7m || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #a855f7;">${(row.lag5to7mPct || 0)}%</td>

            <td style="padding: 1rem;">${(row.lag8to12m || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #f472b6;">${(row.lag8to12mPct || 0)}%</td>

            <td style="padding: 1rem;">${(row.lagGt12m || 0).toLocaleString()}</td>
            <td style="padding: 1rem; color: #ef4444;">${(row.lagGt12mPct || 0)}%</td>
        `;

        tr.innerHTML = html;
        businessMatrixTableBody.appendChild(tr);
    });
}

// Device Data (Injected)
const deviceData = [
    {
        "month": "2025-01",
        "newDevices": 10624
    },
    {
        "month": "2025-02",
        "newDevices": 9553
    },
    {
        "month": "2025-03",
        "newDevices": 9689
    },
    {
        "month": "2025-04",
        "newDevices": 8030
    },
    {
        "month": "2025-05",
        "newDevices": 8287
    },
    {
        "month": "2025-06",
        "newDevices": 9038
    },
    {
        "month": "2025-07",
        "newDevices": 12435
    },
    {
        "month": "2025-08",
        "newDevices": 11932
    },
    {
        "month": "2025-09",
        "newDevices": 14962
    },
    {
        "month": "2025-10",
        "newDevices": 17391
    },
    {
        "month": "2025-11",
        "newDevices": 17589
    },
    {
        "month": "2025-12",
        "newDevices": 27075
    }
];

// 8. Monthly New Device Chart
const deviceCtx = document.getElementById('deviceChart');
if (deviceCtx && typeof deviceData !== 'undefined') {
    const devLabels = deviceData.map(d => d.month);
    const devValues = deviceData.map(d => d.newDevices);

    new Chart(deviceCtx, {
        type: 'bar',
        data: {
            labels: devLabels,
            datasets: [{
                label: 'New Devices',
                data: devValues,
                backgroundColor: '#f97316', // Orange theme
                borderRadius: 6
            }]
        },
        options: {
            ...chartOptions,
            plugins: {
                ...chartOptions.plugins,
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return 'New Devices: ' + context.parsed.y.toLocaleString();
                        }
                    }
                }
            }
        }
    });
}

// Buyer History Data (Injected)
const buyerHistoryData = [
    {
        "month": "2025-01",
        "totalDirectBuyers": 1662,
        "existingUsers": 1214,
        "existingUsersPct": 73.04,
        "activeSubUsers": 436,
        "activeSubUsersPct": 35.91
    },
    {
        "month": "2025-02",
        "totalDirectBuyers": 1648,
        "existingUsers": 1178,
        "existingUsersPct": 71.48,
        "activeSubUsers": 434,
        "activeSubUsersPct": 36.84
    },
    {
        "month": "2025-03",
        "totalDirectBuyers": 1646,
        "existingUsers": 1271,
        "existingUsersPct": 77.22,
        "activeSubUsers": 420,
        "activeSubUsersPct": 33.04
    },
    {
        "month": "2025-04",
        "totalDirectBuyers": 1426,
        "existingUsers": 1126,
        "existingUsersPct": 78.96,
        "activeSubUsers": 373,
        "activeSubUsersPct": 33.13
    },
    {
        "month": "2025-05",
        "totalDirectBuyers": 1406,
        "existingUsers": 1126,
        "existingUsersPct": 80.09,
        "activeSubUsers": 344,
        "activeSubUsersPct": 30.55
    },
    {
        "month": "2025-06",
        "totalDirectBuyers": 1489,
        "existingUsers": 1171,
        "existingUsersPct": 78.64,
        "activeSubUsers": 372,
        "activeSubUsersPct": 31.77
    },
    {
        "month": "2025-07",
        "totalDirectBuyers": 1755,
        "existingUsers": 1345,
        "existingUsersPct": 76.64,
        "activeSubUsers": 493,
        "activeSubUsersPct": 36.65
    },
    {
        "month": "2025-08",
        "totalDirectBuyers": 1815,
        "existingUsers": 1338,
        "existingUsersPct": 73.72,
        "activeSubUsers": 460,
        "activeSubUsersPct": 34.38
    },
    {
        "month": "2025-09",
        "totalDirectBuyers": 1662,
        "existingUsers": 1186,
        "existingUsersPct": 71.36,
        "activeSubUsers": 385,
        "activeSubUsersPct": 32.46
    },
    {
        "month": "2025-10",
        "totalDirectBuyers": 1603,
        "existingUsers": 1148,
        "existingUsersPct": 71.62,
        "activeSubUsers": 402,
        "activeSubUsersPct": 35.02
    },
    {
        "month": "2025-11",
        "totalDirectBuyers": 1607,
        "existingUsers": 1124,
        "existingUsersPct": 69.94,
        "activeSubUsers": 383,
        "activeSubUsersPct": 34.07
    },
    {
        "month": "2025-12",
        "totalDirectBuyers": 1958,
        "existingUsers": 1290,
        "existingUsersPct": 65.88,
        "activeSubUsers": 391,
        "activeSubUsersPct": 30.31
    }
];

// 9. Direct Buyer History Table
const historyTableBody = document.getElementById('historyTableBody');
if (historyTableBody && typeof buyerHistoryData !== 'undefined') {
    buyerHistoryData.forEach(row => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.05)';

        const newUsers = row.totalDirectBuyers - row.existingUsers;
        const newUsersPct = row.totalDirectBuyers > 0 ? ((newUsers / row.totalDirectBuyers) * 100).toFixed(2) : 0;

        // Handle optional activeSubUsers (graceful fallback)
        const activeSub = row.activeSubUsers || 0;
        const activeSubPct = row.activeSubUsersPct || 0;

        tr.innerHTML = `
            <td style="padding: 1rem;">${row.month}</td>
            <td style="padding: 1rem;">${row.totalDirectBuyers.toLocaleString()}</td>
            <td style="padding: 1rem; color: #facc15;">${row.existingUsers.toLocaleString()}</td>
            <td style="padding: 1rem;">${row.existingUsersPct}%</td>
            <td style="padding: 1rem; color: #60a5fa;">${activeSub.toLocaleString()}</td>
            <td style="padding: 1rem;">${activeSubPct}%</td>
            <td style="padding: 1rem; color: #4ade80;">${newUsers.toLocaleString()}</td>
            <td style="padding: 1rem;">${newUsersPct}%</td>
        `;
        historyTableBody.appendChild(tr);
    });
}
// 10. Active Subscription Data
const activeSubscriptionData = [
    { "month": "2025-01", "activeSubscriptions": 19757 },
    { "month": "2025-02", "activeSubscriptions": 19762 },
    { "month": "2025-03", "activeSubscriptions": 20173 },
    { "month": "2025-04", "activeSubscriptions": 19858 },
    { "month": "2025-05", "activeSubscriptions": 19777 },
    { "month": "2025-06", "activeSubscriptions": 19903 },
    { "month": "2025-07", "activeSubscriptions": 20688 },
    { "month": "2025-08", "activeSubscriptions": 21405 },
    { "month": "2025-09", "activeSubscriptions": 21701 },
    { "month": "2025-10", "activeSubscriptions": 21660 },
    { "month": "2025-11", "activeSubscriptions": 21782 },
    { "month": "2025-12", "activeSubscriptions": 22853 }
];

// 10. Active Subscriptions Chart
const activeSubCtx = document.getElementById('activeSubChart');
if (activeSubCtx && typeof activeSubscriptionData !== 'undefined') {
    const subLabels = activeSubscriptionData.map(d => d.month);
    const subValues = activeSubscriptionData.map(d => d.activeSubscriptions);

    new Chart(activeSubCtx, {
        type: 'bar',
        data: {
            labels: subLabels,
            datasets: [{
                label: 'Active Subscriptions',
                data: subValues,
                backgroundColor: '#22d3ee', // Cyan theme
                borderRadius: 6
            }]
        },
        options: {
            ...chartOptions,
            plugins: {
                ...chartOptions.plugins,
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return 'Valid Subscriptions: ' + context.parsed.y.toLocaleString();
                        }
                    }
                }
            }
        }
    });
}
