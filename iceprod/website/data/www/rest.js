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

async function* fetch_json_streaming(method, url, json, passkey) {
    if (method == 'GET' && json != null) {
        throw "fetch_json(): arument json must be null if method == GET";
    }
    if (passkey == null) {
        throw "fetch_json: passkey can't be null";
    }
    try {
        const payload = {
                'headers': new Headers({
                                'Authorization': 'Bearer ' + passkey,
                                'Content-Type': 'application/json',
                            }),
                'method': method,
                'body': json ? JSON.stringify(json) : null,
        };
        const response = await fetch(rest_api + url, payload);

        // line reader code from https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch#processing_a_text_file_line_by_line
        const utf8Decoder = new TextDecoder("utf-8");
        const reader = response.body.getReader();
        let { value: chunk, done: readerDone } = await reader.read();
        chunk = chunk ? utf8Decoder.decode(chunk) : "";

        const newline = /\r?\n/gm;
        let startIndex = 0;
        let result;

        while (true) {
            const result = newline.exec(chunk);
            if (!result) {
                if (readerDone) break;
                const remainder = chunk.substr(startIndex);
                ({ value: chunk, done: readerDone } = await reader.read());
                chunk = remainder + (chunk ? utf8Decoder.decode(chunk) : "");
                startIndex = newline.lastIndex = 0;
                continue;
            }
            const line = chunk.substring(startIndex, result.index);
            yield JSON.parse(line);
            startIndex = newline.lastIndex;
        }

        if (startIndex < chunk.length) {
            // Last line didn't end in a newline char
            const line = chunk.substr(startIndex);
            yield JSON.parse(line);
        }
    } catch(err) {
        if (!response.ok) {
            console.log('fetch_json(): response failed. '+response.status+': '+response.statusText);
            if (response.status == 404) {
                yield {'error': 'method not found'}
            } else if (response.status >= 500) {
                yield {'error': 'server error'}
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

