/*
 * Helper functions that use REST API
 */

async function fetch_json(method, url, json, passkey) {
    if (method == 'GET' && json != null) {
        throw "fetch_json(): arument json must be null if method == GET";
    }
    if (passkey == null) {
        throw "fetch_json: passkey can't be null";
    }
    try {
        var payload = {
                'headers': new Headers({
                                'Authorization': 'Bearer ' + passkey,
                                'Content-Type': 'application/json',
                            }),
                'method': method,
                'body': json ? JSON.stringify(json) : null,
        };
        var response = await fetch(url, payload);
        return await response.json();
    } catch(err) {
        alert(err);
    }
}

function set_dataset_status(dataset_id, stat, passkey, task_status_filters=[''], propagate=true) {
    if (propagate) {
        let jobs = fetch_json('GET', '/datasets/' + dataset_id + '/jobs', null, passkey);
        set_jobs_status(jobs.keys(), stat, passkey, task_status_filters);
    }
    fetch_json('PUT', '/datasets/' + dataset_id + '/status', {'status':stat}, passkey);
}

function set_jobs_status(job_ids, stat, passkey, task_status_filters=['']) {
    for (jid in job_ids) {
        var job = fetch_json('GET', '/jobs/' + jid, null, passkey);
        for (status_filter in task_status_filters) {
            let filter = status_filter ? '&task_status=' + status_filter : '';
            let tasks = fetch_json('GET',
                            '/datasets/' + job['dataset_id'] + '/tasks?job_id=' + job['job_id'] + filter,
                            null, passkey);
            set_tasks_status(tasks.keys(), stat, passkey);
        }
        fetch_json('PUT', '/datasets/' + job['dataset_id'] + '/jobs/' + jid + '/status', {'status':stat}, passkey);
    }
}

function set_tasks_status(task_ids, stat, passkey) {
    for (tid in task_ids) {
        let task = fetch_json('GET', '/tasks/' + tid, null, passkey);
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/tasks/' + task['task_id'] + '/status',
                        {'status':stat}, passkey);
    }
}

function set_tasks_and_jobs_status(task_ids, stat, passkey) {
    for (tid in task_ids) {
        let task = fetch_json('GET', '/tasks/' + tid, null, passkey);
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/tasks/' + task['task_id'] + '/status',
                    {'status':stat}, passkey);
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/jobs/' + task['job_id'] + '/status',
                    {'status':stat}, passkey);
    }
}
