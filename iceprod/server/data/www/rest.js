/*
 * Helper functions that use REST API
 */

var rest_api = 'https://iceprod2-api.icecube.wisc.edu';
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
        var response = await fetch(rest_api + url, payload);
        return await response.json();
    } catch(err) {
        alert(err);
    }
}

async function set_dataset_status(dataset_id, stat, passkey, task_status_filters=[''], propagate=true) {
    if (propagate) {
        let job_stat = 'reset';
        if (stat == 'suspended' || stat == 'truncated')
            job_stat = 'suspended';
        let jobs = await fetch_json('GET', '/datasets/' + dataset_id + '/jobs', null, passkey);
        if ('error' in jobs) {
            alert('error - '+jobs['error']);
            return;
        }
        let job_ids = Object.keys(jobs);
        if (job_ids.length > 0) {
            let ret = await set_jobs_status(job_ids, job_stat, passkey, task_status_filters);
            if ('error' in ret) {
                alert('error - '+ret['error']);
                return;
            }
        }
    }
    let ret = await fetch_json('PUT', '/datasets/' + dataset_id + '/status', {'status':stat}, passkey);
    if ('error' in ret) {
        alert('error - '+ret['error']);
    }
}

async function set_jobs_status(job_ids, stat, passkey, task_status_filters=['']) {
    for (jid in job_ids) {
        var job = await fetch_json('GET', '/jobs/' + jid, null, passkey);
        if ('error' in job) {
            alert('error - '+job['error']);
            return;
        }
        for (status_filter in task_status_filters) {
            let filter = status_filter ? '&task_status=' + status_filter : '';
            let tasks = await fetch_json('GET',
                            '/datasets/' + job['dataset_id'] + '/tasks?job_id=' + job['job_id'] + filter,
                            null, passkey);
            var task_ids = Object.keys(tasks);
            if (task_ids.length > 0) {
                set_tasks_status(task_ids, stat, passkey);
            }
        }
        fetch_json('PUT', '/datasets/' + job['dataset_id'] + '/jobs/' + jid + '/status', {'status':stat}, passkey);
    }
}

async function set_tasks_status(task_ids, stat, passkey) {
    for (tid in task_ids) {
        let task = await fetch_json('GET', '/tasks/' + tid, null, passkey);
        if ('error' in task) {
            alert('error - '+task['error']);
            return;
        }
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/tasks/' + task['task_id'] + '/status',
                        {'status':stat}, passkey);
    }
}

async function set_tasks_and_jobs_status(task_ids, stat, passkey) {
    for (tid in task_ids) {
        let task = await fetch_json('GET', '/tasks/' + tid, null, passkey);
        if ('error' in task) {
            alert('error - '+task['error']);
            return;
        }
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/tasks/' + task['task_id'] + '/status',
                    {'status':stat}, passkey);
        fetch_json('PUT', '/datasets/' + task['dataset_id'] + '/jobs/' + task['job_id'] + '/status',
                    {'status':stat}, passkey);
    }
}
