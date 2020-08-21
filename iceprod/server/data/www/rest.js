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
        if (!response.ok) {
            console.log('fetch_json(): response failed. '+response.status+': '+response.statusText);
            if (response.status == 404) {
                return {'error': 'method not found'}
            } else if (response.status >= 500) {
                return {'error': 'server error'}
            }
        } else {
            console.log('fetch_json(): method='+method);
            console.log('fetch_json(): url='+rest_api + url);
            console.log('fetch_json(): err='+err);
        }
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

async function set_dataset_priority(dataset_id, passkey) {
    try {
        let val = parseFloat($("#dataset_priority").val());
        
        let url = '/datasets/' + dataset_id + '/priority';
        let ret = await fetch_json('PUT', url, {'priority': val}, passkey);
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
            let ret = await set_jobs_status(dataset_id, job_ids, job_stat, passkey, [], false, false);
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
            let ret = await set_tasks_status(dataset_id, task_ids, task_stat, passkey, false);
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

async function set_jobs_status(dataset_id, job_ids, stat, passkey, task_status_filters=[], propagate=true, messaging=true) {
    let tmpjobids = job_ids;
    while (tmpjobids.length > 0) {
        let curjobids = tmpjobids.slice(0,50000);
        tmpjobids = tmpjobids.slice(50000);
        let ret = await fetch_json('POST', '/datasets/' + dataset_id + '/job_actions/bulk_status/'+stat,
                        {'jobs':curjobids}, passkey);
        if ('error' in ret) {
            message_alert('error - '+ret['error']);
            return false;
        }
    }

    if (propagate) {
        if (messaging) {
            message('getting task_ids - 0% complete');
        }
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
            if (messaging && i%100 == 0) {
                message('getting task_ids - '+Math.floor(i/job_ids.length)+'% complete');
            }
            let promises = [];
            let j=i;
            for (;j<i+10 && j<job_ids.length;j++) {
                let url = base_url+'job_id='+job_ids[i];
                promises.push(fetch_json('GET', url, null, passkey));
            }
            let results = await Promise.all(promises);
            for (var k=0;k<results.length;k++) {
                let ret = Object.keys(results[k]);
                if (ret.length > 0) {
                    task_ids.push.apply(task_ids, ret);
                }
            }
        }
        if (task_ids.length > 0) {
            if (messaging) {
                message("updating tasks");
            }
            let ret = await set_tasks_status(dataset_id, task_ids, task_status, passkey, false);
            if (ret == false)
                return false
        }
        if (messaging) {
            message_close();
        }
    }
    return true;
}

async function set_tasks_status(dataset_id, task_ids, stat, passkey, messaging=true) {
    let total_tasks = task_ids.length;
    try {
        while (task_ids.length > 0) {
            if (messaging) {
                message("updating tasks - "+Math.floor(1-task_ids.length/total_tasks)+"% complete");
            }
            let curtaskids = task_ids.slice(0,50000);
            task_ids = task_ids.slice(50000);
            let ret = await fetch_json('POST', '/datasets/' + dataset_id + '/task_actions/bulk_status/'+stat,
                            {'tasks':curtaskids}, passkey);
            if ('error' in ret) {
                message_alert('error - '+ret['error']);
                return false;
            }
        }
    } catch(err) {
        message_alert('error - '+err);
        return false;
    }
    if (messaging) {
        message_close();
    }
    return true;
}

async function set_tasks_and_jobs_status(dataset_id, task_ids, stat, passkey, messaging=true) {
    var job_status = stat;
    if (stat == "idle" || stat == "waiting" || stat == "queued" || stat == "processing" || stat == "reset")
        job_status = "processing";
    for (var i=0;i<task_ids.length;i++) {
        if (messaging && i%10 == 0) {
            message('updating tasks and jobs - '+Math.floor(i/task_ids.length)+'% complete');
        }
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
    if (messaging) {
        message_close();
    }
}

async function delete_dataset_logs(dataset_id, passkey) {
    message("Getting task_ids to delete...");
    let url = '/datasets/' + dataset_id + '/tasks?keys=task_id';
    let tasks = await fetch_json('GET', url, null, passkey);
    if ('error' in tasks) {
        message_alert('tasks error - '+tasks['error']);
        return false;
    }
    let task_ids = Object.keys(tasks);
    if (task_ids.length > 0) {
        let ret = await delete_task_logs(dataset_id, task_ids, passkey);
        if (ret == false) {
            return false;
        }
    }
    message_close();
    return true;
}

async function delete_task_logs(dataset_id, task_ids, passkey, messaging=true) {
    for (var i=0;i<task_ids.length;i++) {
        if (messaging && i%10 == 0) {
            message('deleting logs - '+Math.floor(i/task_ids.length)+'% complete');
        }
        let tid = task_ids[i];
        let ret = await fetch_json('DELETE', '/datasets/' + dataset_id + '/tasks/' + tid + '/logs', null, passkey);
        if ('error' in ret) {
            message_alert('error - '+ret['error']);
            return false;
        }
    }
    if (messaging) {
        message_close();
    }
    return true
}
