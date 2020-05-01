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
        console.log('fetch_json(): method='+method);
        console.log('fetch_json(): url='+rest_api + url);
        console.log('fetch_json(): err='+err);
        throw err;
    }
}

function message(text="", border_color='#666') {
    let msgbox = $("#message-outer");
    if (msgbox.length == 0) {
        // create new msgbox
        $("body").append('<div id="message-outer"><div id="message-inner"><p></p><button class="close">x</button></div></div>');
        $('#message-inner button.close').on('click', message_close);
    }
    console.log(text);
    $('#message-inner p').html(text);
    $('#message-inner').css("border-color", border_color);
}
function message_alert(text) {
    return message(text, border_color='red');
}
function message_close() {
    let msgbox = $("#message-outer");
    if (msgbox.length > 0) {
        msgbox.remove();
    }
}

function reload() {
    window.location.reload();
}

const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}


async function set_dataset_status(dataset_id, stat, passkey, task_status_filters=[], job_status_filters=[], propagate=true) {
    if (propagate) {
        let job_stat = 'processing';
        let task_stat = 'reset';
        if (stat == 'suspended') {
            job_stat = 'suspended';
            task_stat = 'suspended';
        }

        message("Updating jobs...");
        let url = '/datasets/' + dataset_id + '/jobs?';
        url += 'status='+job_status_filters[0];
        for (var i=1;i<job_status_filters.length;i++) {
            url += '|'+job_status_filters[i];
        }
        url += '&keys=job_id';
        let jobs = await fetch_json('GET', url, null, passkey);
        if ('error' in jobs) {
            message_alert('jobs error - '+jobs['error']);
            return false;
        }
        let job_ids = Object.keys(jobs);
        if (job_ids.length > 0) {
            let ret = await set_jobs_status(dataset_id, job_ids, job_stat, passkey, [], false);
            if (ret == false) {
                return false;
            }
        }

        message("Updating tasks...");
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
            message_alert('tasks error - '+tasks['error']);
            return false;
        }
        let task_ids = Object.keys(tasks);
        if (task_ids.length > 0) {
            let ret = await set_tasks_status(dataset_id, task_ids, task_stat, passkey);
            if (ret == false) {
                return false;
            }
        }
    }
    message("Updating dataset status");
    let ret = await fetch_json('PUT', '/datasets/' + dataset_id + '/status', {'status':stat}, passkey);
    if ('error' in ret) {
        message_alert('datasets error - '+ret['error']);
        return false;
    }
    message_close();
    return true;
}

async function set_jobs_status(dataset_id, job_ids, stat, passkey, task_status_filters=[], propagate=true) {
    let ret = await fetch_json('POST', '/datasets/' + dataset_id + '/job_actions/bulk_status/'+stat,
                    {'jobs':job_ids}, passkey);
    if ('error' in ret) {
        message_alert('error - '+ret['error']);
        return false;
    }

    if (propagate) {
        let task_status = 'suspended';
        if (stat == 'processing')
            task_status = 'reset';
        let base_url = '/datasets/' + dataset_id + '/tasks?keys=task_id&';
        if (task_status_filters.length > 0) {
            base_url += 'status='+task_status_filters[0];
            for (var j=1;j<task_status_filters.length;j++) {
                base_url += '|'+task_status_filters[j];
            }
            base_url += '&';
        }
        let task_ids = [];
        // do 10 jobs at once for parallelization
        for (var i=0;i<job_ids.length;i+=10) {
            let promises = [];
            let j=i;
            for (;j<i+10 && j<job_ids.length;j++) {
                let url = base_url+'job_id='+job_ids[i];
                promises.push(fetch_json('GET', url, null, passkey));
            }
            let results = await Promise.all(promises);
            for (var k=i;k<j;k++) {
                let ret = Object.keys(results[k]);
                if (ret.length > 0) {
                    task_ids.push.apply(ret);
                }
            }
        }
        if (task_ids.length > 0) {
            return set_tasks_status(dataset_id, task_ids, task_status, passkey);
        }
    }
    return true;
}

async function set_tasks_status(dataset_id, task_ids, stat, passkey) {
    try {
        let ret = await fetch_json('POST', '/datasets/' + dataset_id + '/task_actions/bulk_status/'+stat,
                        {'tasks':task_ids}, passkey);
        if ('error' in ret) {
            message_alert('error - '+ret['error']);
            return false;
        }
    } catch(err) {
        message_alert('error - '+err);
        return false;
    }
    return true;
}

async function set_tasks_and_jobs_status(dataset_id, task_ids, stat, passkey) {
    var job_status = stat;
    if (stat == "idle" || stat == "waiting" || stat == "queued" || stat == "processing" || stat == "reset")
        job_status = "processing";
    for (var i=0;i<task_ids.length;i++) {
        let tid = task_ids[i];
        let task = await fetch_json('GET', '/datasets/' + dataset_id + '/tasks/' + tid, null, passkey);
        if ('error' in task) {
            message_alert('error - '+task['error']);
            return;
        }
        let fut = fetch_json('PUT', '/datasets/' + dataset_id + '/tasks/' + tid + '/status',
                    {'status':stat}, passkey);
        let fut2 = fetch_json('PUT', '/datasets/' + dataset_id + '/jobs/' + task['job_id'] + '/status',
                    {'status':job_status}, passkey);
        let ret = await fut;
        if ('error' in ret) {
            message_alert('error - '+ret['error']);
            return;
        }
        ret = await fut2;
        if ('error' in ret) {
            message_alert('error - '+ret['error']);
            return;
        }
    }
}
