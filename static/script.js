document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('analysis-form');
    const resultsContainer = document.getElementById('results-container');
    const loadingIndicator = document.getElementById('loading');
    const resultsContent = document.getElementById('results-content');
    const errorContainer = document.getElementById('error');
    const summaryEl = document.getElementById('summary');
    const detailsEl = document.getElementById('details');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        // Hide previous results and show loading indicator
        resultsContainer.classList.remove('hidden');
        resultsContent.classList.add('hidden');
        errorContainer.classList.add('hidden');
        loadingIndicator.classList.remove('hidden');

        const formData = new FormData();
        const region = document.getElementById('region').value;
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        const selectedDomains = Array.from(document.querySelectorAll('input[name="domains"]:checked'))
                                     .map(el => el.value);

        let q = `${region}`;
        if (startDate && endDate) {
            q += ` ${startDate} 부터 ${endDate} 까지`;
        } else if (startDate) {
            q += ` ${startDate} 에`;
        } else if (endDate) {
            q += ` ${endDate} 에`;
        }
        
        formData.append('q', q);
        formData.append('domains', selectedDomains.join(','));
        formData.append('include_summary', '1');

        try {
            const response = await fetch('/api/analysis/report', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error?.message || `HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            displayResults(data);

        } catch (error) {
            displayError(error.message);
        } finally {
            loadingIndicator.classList.add('hidden');
        }
    });

    function displayResults(data) {
        // Clear previous details
        detailsEl.innerHTML = '';

        // Show summary
        summaryEl.textContent = data.summary || '요약이 생성되지 않았습니다.';
        
        // Process and display detailed results
        if (data.results) {
            Object.keys(data.results).forEach(key => {
                const result = data.results[key];
                const section = document.createElement('div');
                const title = document.createElement('h4');
                title.textContent = formatTitle(key);
                section.appendChild(title);

                if (result && Array.isArray(result) && result.length > 0) {
                    section.appendChild(createTable(result));
                } else if (result && typeof result === 'object' && Object.keys(result).length > 0) {
                     if (result.rows && result.rows.length > 0) {
                        section.appendChild(createTable(result.rows));
                     } else if (Object.keys(result).length > 0) {
                        const pre = document.createElement('pre');
                        pre.textContent = JSON.stringify(result, null, 2);
                        section.appendChild(pre);
                     } else {
                        section.appendChild(createNoResultMessage());
                     }
                } else {
                    section.appendChild(createNoResultMessage());
                }
                detailsEl.appendChild(section);
            });
        }


        resultsContent.classList.remove('hidden');
    }

    function displayError(message) {
        errorContainer.textContent = `오류가 발생했습니다: ${message}`;
        errorContainer.classList.remove('hidden');
    }

    function createTable(data) {
        const table = document.createElement('table');
        const thead = document.createElement('thead');
        const tbody = document.createElement('tbody');
        const headerRow = document.createElement('tr');

        // Create headers
        const headers = Object.keys(data[0]);
        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Create rows
        data.forEach(rowData => {
            const row = document.createElement('tr');
            headers.forEach(header => {
                const td = document.createElement('td');
                let value = rowData[header];
                if (typeof value === 'number' && !Number.isInteger(value)) {
                    value = value.toFixed(3);
                }
                td.textContent = value;
                row.appendChild(td);
            });
            tbody.appendChild(row);
        });
        table.appendChild(tbody);
        return table;
    }
    
    function formatTitle(key) {
        const titles = {
            'compare_domains': '도메인 비교',
            'detect_anomaly': '이상 징후 탐지',
            'get_rankings': '순위',
            'get_advanced_insight': '고급 인사이트'
        };
        return titles[key] || key;
    }

    function createNoResultMessage() {
        const p = document.createElement('p');
        p.textContent = '결과가 없습니다.';
        return p;
    }
});
