document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', () => {
            const moduleId = item.dataset.module;

            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            item.classList.add('active');

            document.querySelectorAll('.module').forEach(el => el.classList.remove('active'));
            document.getElementById(`module-${moduleId}`).classList.add('active');

            if (moduleId === 'stages') loadStages();
            if (moduleId === 'apps') loadApps();
            if (moduleId === 'deploy') loadDeployments();
            if (moduleId === 'strategies') loadStrategies();
            if (moduleId === 'experiments') loadExperiments();
            if (moduleId === 'submit') loadSubmitOptions();
            if (moduleId === 'records') loadRecords();
        });
    });

    loadStages();
});
