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

async function set_dataset_status(dataset_id, stat, passkey, task_status_filters=[], job_status_filters=[], propagate=true) {
    if (propagate) {
        let job_stat = 'processing';
        let task_stat = 'reset';
        if (stat == 'suspended') {
            job_stat = 'suspended';
            task_stat = 'suspended';
        }
        let url = '/datasets/' + dataset_id + '/jobs?';
        url += 'status='+job_status_filters[0];
        for (var i=1;i<job_status_filters.length;i++) {
            url += '|'+job_status_filters[i];
        }
        url += '&keys=job_id';
        let jobs = await fetch_json('GET', url, null, passkey);
        if ('error' in jobs) {
            alert('jobs error - '+jobs['error']);
            return;
        }
        let job_ids = Object.keys(jobs);
        if (job_ids.length > 0) {
            let ret = await set_jobs_status(dataset_id, job_ids, job_stat, passkey, [''], false);
            if (ret && 'error' in ret) {
                alert('jobs error - '+ret['error']);
                return;
            }
        }
        url = '/datasets/' + dataset_id + '/tasks?';
        if (task_status_filters.length > 0) {
            url += 'status='+task_status_filters[0];
            for (var i=1;i<task_status_filters.length;i++) {
                url += '|'+task_status_filters[i];
            }
        }
        url += '&keys=task_id';
        
        let tasks = await fetch_json('GET', url, null, passkey);
        if ('error' in tasks) {
            alert('tasks error - '+tasks['error']);
            return;
        }
        let task_ids = Object.keys(tasks);
        if (task_ids.length > 0) {
            set_tasks_status(dataset_id, task_ids, task_stat, passkey);
        }
    }
    let ret = await fetch_json('PUT', '/datasets/' + dataset_id + '/status', {'status':stat}, passkey);
    if ('error' in ret) {
        alert('datasets error - '+ret['error']);
    }
}

async function set_jobs_status(dataset_id, job_ids, stat, passkey, task_status_filters=[], propagate=true) {
    var task_status = 'suspended';
    if (stat == 'processing')
        task_status = 'reset';
    for (var i=0;i<job_ids.length;i++) {
        let jid = job_ids[i];
        if (propagate) {
            let url = '/datasets/' + dataset_id + '/tasks?job_id='+jid;
            if (task_status_filters.length > 0) {
                url += '&status='+task_status_filters[0];
                for (var i=1;i<task_status_filters.length;i++) {
                    url += '|'+task_status_filters[i];
                }
            }
            url += '&keys=task_id';
            let tasks = await fetch_json('GET', url, null, passkey);
            var task_ids = Object.keys(tasks);
            if (task_ids.length > 0) {
                set_tasks_status(dataset_id, task_ids, task_status, passkey);
            }
        }
        let ret = await fetch_json('PUT', '/datasets/' + dataset_id + '/jobs/' + jid + '/status', {'status':stat}, passkey);
        if ('error' in ret) {
            alert('error - '+ret['error']);
            return ret;
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
