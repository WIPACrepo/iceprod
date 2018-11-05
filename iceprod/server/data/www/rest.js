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
        alert('fetch_json(): '+err);
        return {};
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
            let ret = await set_jobs_status(dataset_id, job_ids, job_stat, passkey, task_status_filters);
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

async function set_jobs_status(dataset_id, job_ids, stat, passkey, task_status_filters=['']) {
    var task_status = stat;
    if (stat == 'processing')
        task_status = 'waiting';
    for (var i=0;i<job_ids.length;i++) {
        let jid = job_ids[i];
        for (var j=0;j<task_status_filters.length;j++) {
            let filter = task_status_filters[j] ? '&task_status=' + task_status_filters[j] : '';
            let tasks = await fetch_json('GET',
                            '/datasets/' + dataset_id + '/tasks?job_id=' + jid + filter,
                            null, passkey);
            var task_ids = Object.keys(tasks);
            if (task_ids.length > 0) {
                set_tasks_status(dataset_id, task_ids, task_status, passkey);
            }
        }
        let ret = await fetch_json('PUT', '/datasets/' + dataset_id + '/jobs/' + jid + '/status', {'status':stat}, passkey);
        if ('error' in ret) {
            alert('error - '+ret['error']);
            return;
        }
    }
}

async function set_tasks_status(dataset_id, task_ids, stat, passkey) {
    let ret = await fetch_json('POST', '/datasets/' + dataset_id + '/task_actions/bulk_status/'+stat,
                    {'tasks':task_ids}, passkey);
    if ('error' in ret) {
        alert('error - '+ret['error']);
        return;
    }
}

async function set_tasks_and_jobs_status(dataset_id, task_ids, stat, passkey) {
    var job_status = stat;
    if (stat == "idle" || stat == "waiting" || stat == "queued" || stat == "processing" || stat == "reset")
        job_status = "processing";
    for (var i=0;i<task_ids.length;i++) {
        let tid = task_ids[i];
        let task = await fetch_json('GET', '/datasets/' + dataset_id + '/tasks/' + tid, null, passkey);
        if ('error' in task) {
            alert('error - '+task['error']);
            return;
        }
        let fut = fetch_json('PUT', '/datasets/' + dataset_id + '/tasks/' + tid + '/status',
                    {'status':stat}, passkey);
        let fut2 = fetch_json('PUT', '/datasets/' + dataset_id + '/jobs/' + task['job_id'] + '/status',
                    {'status':job_status}, passkey);
        let ret = await fut;
        if ('error' in ret) {
            alert('error - '+ret['error']);
            return;
        }
        ret = await fut2;
        if ('error' in ret) {
            alert('error - '+ret['error']);
            return;
        }
    }
}
