/*!
 * Dataset submission tools
 */

var Submission = (function( $ ) {

    var data = {
        element : null,
        state : 'expert',
        passkey : null,
        edit : false,
        dataset : null,
        submit_data : {},
        rest_api : ''
    };

    function pluralize(value) {
        var ret = "" + value;
        if (ret in dataclasses['names'])
            return dataclasses['names'][ret];
        else
            return ret;
    }
    function singular(value) {
        var ret = "" + value;
        if (value.toLowerCase() == 'categories' || value.toLowerCase() == 'category' || value == '')
            console.warn('singular('+value+')');
        for (d in dataclasses['names']) {
            if (ret.toLowerCase() == dataclasses['names'][d].toLowerCase())
                return d;
        }
        return ret;
    }
    function getDataclass(path,key) {
        console.log('getDataclass('+path+' , '+key+')')
        if ((path === null || path == '') && !(key === undefined))
            path = key;
        else if (key != null && key != '')
            path = path + '.' + key;
        if (path === null || path === undefined)
            path = '';
        var depths = path.split('.'), d = dataclasses['classes']['Job'],
            part = [], p = '', c = null;
        for (var i=0;i<depths.length;i++) {
            part = depths[i].split('_');
            if (part.length > 1) {
                p = part.slice(0,-1).join('_');
            } else
                p = part[0]
            if (p in d && $.type(d[p]) == 'array') {
                c = d[p][1];
                d = dataclasses['classes'][c]
            } else
                return null;
        }
        if (c == '')
            return null;
        return c;
    }

    var private_methods = {
        clean_json : function(j) {
            var json = j;
            if (j === undefined)
                json = data.submit_data;
            if ($.type(json) === 'array') {
                for (var i=0;i<json.length;i++)
                    json[i] = private_methods.clean_json(json[i]);
            } else if ($.type(json) === 'object') {
                var ret_json = {};
                for (var i in json) {
                    if (i == '_type')
                        continue;
                    ret_json[i] = private_methods.clean_json(json[i]);
                }
                json = ret_json;
            }
            if (j === undefined)
                data.submit_data = json;
            else
                return json;
        },
        json_type_markup : function(j,t) {
            var json = j;
            if (j === null) {
                console.log('markup null');
                return j;
            }
            if (j === undefined && t === undefined) {
                console.log('markup undefined');
                json = data.submit_data;
                console.log(json);
                t = 'Job';
            }
            if ($.type(json) === 'array') {
                for (var i=0;i<json.length;i++) {
                    json[i] = private_methods.json_type_markup(json[i],t);
                }
            } else if ($.type(json) === 'object') {
                if (t in dataclasses['classes']) {
                    var parent = dataclasses['classes'][t];
                    for (var i in json) {
                        if (i in parent && $.type(parent[i]) === 'array')
                            json[i] = private_methods.json_type_markup(json[i],parent[i][1]);
                    }
                    json['_type'] = t;
                }
            }
            if (j === undefined)
                data.submit_data = json;
            else
                return json;
        },
        // XXX gridspec is not used
        submit : async function(num_jobs, gridspec, description) {
            private_methods.clean_json();
            response = await fetch_json('POST', data.rest_api + '/datasets',
                            {'description': description,
                                'jobs_submitted': num_jobs,
                                'tasks_submitted': num_jobs * data.submit_data['tasks'].length, // XXX is this right?
                                'group_id': 'NOT IMPLEMENTED YET',
                            }, data.passkey);
            data.dataset.dataset_id = response['result'].split('/')[2];
            await fetch_json('PUT', data.rest_api + '/config/' + data.dataset.dataset_id, data.submit_data, data.passkey);
            window.location = '/dataset/' + data.dataset.dataset_id;
        },
        update : function(description) {
            private_methods.clean_json();
            fetch_json('PUT', data.rest_api + '/config/' + data.dataset.dataset_id, data.submit_data, data.passkey);
            fetch_json('PUT', data.rest_api + '/datasets/' + data.dataset.dataset_id + '/description', description, data.passkey);
        },
        build_advanced : function( ) {
            private_methods.json_type_markup();
            var editing = (function(){
                var keys = {};
                var methods = {
                    is: function(key,path) {
                        if (!(path === undefined) && path != null && path != '')
                            key = path+'.'+key;
                        console.log('editing.is() key='+key);
                        if (key in keys)
                            return keys[key];
                        return keys[key] = false;
                    },
                    set: function(key,val,path) {
                        console.warn('editing.set('+key+' , '+val+' , '+path+')');
                        if (!(path === undefined) && path != null && path != '')
                            if (key != null && key != '')
                                key = path+'.'+key;
                            else
                                key = path;
                        console.log('editing.set() key='+key);
                        keys[key] = val;
                    },
                    clearall: function() {
                        keys = {};
                    }
                };
                return methods;
            })();
            var converters = {
                intToStr: function(value) {
                    if (value == null || value == undefined)
                        return ""
                    else
                        return "" + value;
                },
                strToInt: function(value) {
                    if (value.toLowerCase() == 'true')
                        return true;
                    else if (value.toLowerCase()  == 'false')
                        return false;
                    else if (value != '' && !isNaN(value)) {
                        if (value.indexOf('.') > -1)
                            return parseFloat(value);
                        else
                            return parseInt(value);
                    } else
                        return value;
                },
                keyToHtml: function(value) {
                    return "" + value.replace('_',' ');
                },
                singular: singular
            };
            var helpers = {
                editable: editing.is,
                equal: function(a,b){
                    var ret = (a == b);
                    console.warn('equal('+a+','+b+'):'+ret);
                    return ret;
                },
                isNull: function(input){
                    return input === null;
                },
                isArray: function(input){
                    return $.type(input) === 'array';
                },
                isBasicArray: function(input){
                    ret =  (input.length > 0 && $.type(input[0]) !== 'object');
                    console.log('isBasicArray '+ret)
                    return ret;
                },
                isObject: function(input){
                    return $.type(input) === 'object';
                },
                isNotPrivate: function(input){
                    return input.charAt(0) != '_';
                },
                isEnum: function(path,key){
                    var c = getDataclass(path);
                    if (c === null || !(c in dataclasses['classes'])) {
                        console.log('isEnum(): dataclass missing');
                        return false;
                    }
                    c = dataclasses['classes'][c];
                    if (!(key in c))
                        return false;
                    var ret= ($.type(c[key]) === 'array' && $.type(c[key][1] === 'array') && c[key][1].length > 0);
                    console.log('isEnum(): '+ret);
                    return ret;
                },
                isDocumented: function(path, key) {
                  key = singular(key).toLowerCase();
                  console.log('11' + key);
                  var types = ['class', 'data', 'dif', 'difplus', 'job', 'module', 'personnel', 'plus', 'resource', 'steering', 'task', 'tray', ];
                  return types.indexOf(key) != -1;
                },
                getEnums: function(path,key){
                    var c = getDataclass(path);
                    if (c === null || !(c in dataclasses['classes']))
                        return [];
                    return dataclasses['classes'][c][key][1];
                },
                concat: function(a,b,c){
                    if (b === undefined)
                        return a;
                    else {
                        if (a != '')
                            a += '.';
                        if (c === undefined || c === null || c == '')
                            return a+b;
                        else
                            return a+c+'_'+b;
                    }
                },
                get_class: function(path,key){
                    var ret = getDataclass(path,key);
                    if (ret === null)
                        return key;
                    else
                        return ret;
                },
                canEditObjectName: function(path,key){
                    var c = getDataclass(path,key), s = null;
                    if (c in dataclasses['classes'])
                        return false;
                    do {
                        s = path.split('.');
                        key = s.slice(-1)[0];
                        path = s.slice(0, -2).join('.')+s.slice(-2.-1)[0].split('_');
                        c = getDataclass(path,key);
                        if (c in dataclasses['classes']) {
                            c = dataclasses['classes'][c];
                            var keys = Object.keys(c);
                            ret = (keys.length == 1 && '*' in c);
                            return ret;
                        }
                    } while (path != null && path.indexOf('.') != -1);
                    return false;
                },
                canAddToObject: function(path,key){
                    var c = getDataclass(path,key), s = null;
                    if (c in dataclasses['classes']) {
                        c = dataclasses['classes'][c];
                        var keys = Object.keys(c);
                        ret = (keys.length == 1 && '*' in c);
                        return ret;
                    }
                    return true;
                }
            };
            $.views.helpers(helpers);
            $.views.converters(converters);
            $.templates({
                AdvTmpl:"#AdvTmpl"
            });
            var id = '#advanced_submit';
            $(id).off().empty();
            $.link.AdvTmpl(id,data.submit_data)
            .on('click','.editable span',function(){
                var parent = $(this).parent().parent().parent(),
                    key = $(this).parent().find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text();
                if ($(this).hasClass('key') && $(this).parent().hasClass('key_editable'))
                    editing.set(key+"_key",true,path);
                else
                    editing.set(key,true,path);
                $.view(id, true, 'data').refresh();
                window.setTimeout(function(){
                    var input = $(id).find('.editing input').first();
                    if (input.length > 0) {
                        input.focus();
                        var strLength = input.val().length;
                        input[0].setSelectionRange(strLength,strLength);
                    } else {
                        input = $(id).find('.editing select').first();
                        if (input.length > 0) {
                            input.focus();
                        }
                        else
                            console.warn('no input for key:'+key);
                    }
                },100);
            })
            .on('blur','.editing',function(){
                var parent = $(this).parent().parent(),
                    key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text();
                if ((key === null || key == '') && path != null && path != '')  {
                    // this is a basic array
                    var val = converters.strToInt($(this).find('input').val()),
                        depths = path.split('.'), d = data.submit_data,
                        c = 0, part = [], p = '', i = 0;
                    for (;i<depths.length-1;i++) {
                        part = depths[i].split('_');
                        if (part.length > 1) {
                            p = part.slice(0,-1).join('_');
                            c = parseInt(part.slice(part.length-1)[0],10);
                            d = d[p][c];
                        } else
                            d = d[part[0]];
                    }
                    part = depths[i].split('_');
                    if (part.length > 1) {
                        p = part.slice(0,-1).join('_');
                        c = parseInt(part.slice(part.length-1)[0],10);
                        d[p][c] = val;
                    } else
                        d[part[0]] = val;
                } else if ($(this).find('select').length > 0) {
                    // this is an enum
                    var val = converters.strToInt($(this).find('select option:selected').text()),
                        depths = path.split('.'), d = data.submit_data,
                        c = 0, part = [], p = '', i = 0;
                    for (;i<depths.length;i++) {
                        part = depths[i].split('_');
                        if (part.length > 1) {
                            p = part.slice(0,-1).join('_');
                            c = parseInt(part.slice(part.length-1)[0],10);
                            d = d[p][c];
                        } else
                            d = d[part[0]];
                    }
                    d[key] = val;
                }
                // else, this is taken care of by jsviews
                editing.clearall();
                $.view(id, true, 'data').refresh();
            })
            .on('blur','.editing_key',function(){
                var parent = $(this).parent().parent(),
                    key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text(),
                    depths = path.split('.'), d = data.submit_data,
                    c = 0, part = [], p = '';
                if (!(path === undefined) && path != null && path != '') {
                    for (var i=0;i<depths.length;i++) {
                        part = depths[i].split('_');
                        if (part.length > 1) {
                            p = part.slice(0,-1).join('_');
                            c = parseInt(part.slice(part.length-1)[0],10);
                            d = d[p][c];
                        } else
                            d = d[part[0]];
                    }
                }
                var newkey = $(this).find('input').val();
                if (newkey != key) {
                    d[newkey] = d[key];
                    delete d[key];
                }
                editing.clearall();
                $.view(id, true, 'data').refresh();
            })
            .on('click','.add',function(){
                var parent = $(this).parent().parent(), key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text(),
                    depths = path.split('.'), d = data.submit_data,
                    c = 0, part = [], p = '';
                if (!(path === undefined) && path != null && path != '') {
                    for (var i=0;i<depths.length;i++) {
                        part = depths[i].split('_');
                        if (part.length > 1) {
                            p = part.slice(0,-1).join('_');
                            c = parseInt(part.slice(part.length-1)[0],10);
                            d = d[p][c];
                        } else
                            d = d[part[0]];
                    }
                }
                console.log('add() path='+path+'   key='+key);
                console.log(d);
                var obj = private_methods.insert_dataclass(d,key);
                if (obj === undefined || obj == null)
                    obj = '';
                if ($(this).hasClass('null')) {
                    console.log('add() null');
                    console.log(d[key]);
                    d[key] = obj;
                    console.log(d[key]);
                    $.view(id, true, 'data').refresh();
                } else if ($(this).hasClass('array')) {
                    console.log('add() array')
                    if (d[key].length < 1) {
                        d[key].push(obj);
                        $.view(id, true, 'data').refresh();
                    } else
                        $.observable(d[key]).insert(obj);
                } else if ($(this).hasClass('object')) {
                    console.log('add() object')
                    console.log(obj)
                    d[key]['newkey'] = obj;
                    $.view(id, true, 'data').refresh();
                }
            });
            $('body').on('blur',function(){
                editing.clearall();
            });
        },
        new_dataclass : function( t ) {
            // return a new dataclass of type t
            if (!(t in dataclasses['classes'])) {
                console.log(t+' not in dataclasses');
                return undefined;
            }
            console.log('making new dataclass: '+t);
            var target = dataclasses['classes'][t], ret = {'_type':t};
            for (var k in target) {
                if ($.type(target[k]) === 'array')
                    ret[k] = target[k][0];
                else
                    ret[k] = target[k];
            }
            if ($.type(ret) === 'object' && '*' in ret) {
                delete ret['*'];
                delete ret['_type'];
            }
            console.log(ret);
            return ret;
        },
        insert_dataclass : function( parent, k ) {
            // return a new dataclass for the key in parent
            var target = {};
            if ($.type(parent) === 'string')
                target = dataclasses['classes'][parent];
            else if ('_type' in parent)
                target = dataclasses['classes'][parent['_type']];
            if ('*' in target && !(k in target))
                k = '*'
            if (k in target && $.type(target[k]) === 'array') {
                var ret = target[k][1], t = $.type(ret);
                if (t === 'string' && ret in dataclasses['classes']) {
                    ret = private_methods.new_dataclass(ret);
                } else if (t === 'object') {
                    console.log('making new dict');
                    ret = $.extend(true,{},ret);
                    if ('*' in ret)
                        delete ret['*'];
                }
                console.log('making new:');
                console.log(ret);
                return ret;
            }
            return undefined;
        }
    };

    var public_methods = {
        init : function(args) {
            if (args == undefined) {
                throw new Error('must supply args');
                return;
            }
            // think about storing state in a cookie
            data.edit = args.edit;
            if (!data.edit)
                data.state = 'basic';
            if ('dataset' in args)
                data.dataset = args.dataset;
            else
                data.dataset = null;
            if ('passkey' in args)
                data.passkey = args.passkey;
            else
                data.passkey = null;
            if ('grids' in args)
                data.grids = args.grids;
            else
                data.grids = null;
            data.element = $(args.element);
            if ('config' in args)
                data.submit_data = args.config;
            else
                data.submit_data = private_methods.new_dataclass('Job')
            if ('rest_api' in args)
                data.rest_api = args.rest_api;
            else
                data.rest_api = 'https://iceprod2-api.icecube.wisc.edu'
            private_methods.clean_json();

            

            var html = '';
            html += '<ul class="tab_bar">';
            html += '<li><button id="basic_button" class="active" >Basic View</button></li>';
            html += '<li><button id="advanced_button">Advanced View</button></li>';
            html += '<li><button id="expert_button">Expert View</button></li>';
            html += '</ul>';    
            html += '<div class="submit_contents">';
            html += '<div id="basic_submit">';
            html += '<form class="table_form">';
		    html += '<textarea id="script_url" placeholder="Script URL"></textarea>';
		    html += '<textarea id="arguments" placeholder="Arguments"></textarea>';
		    html += '<textarea id="data_input" placeholder="Data Input"></textarea>';
		    html += '<textarea id="data_output" placeholder="Data Output"></textarea>';
		    html += '<textarea id="env_shell" placeholder="Env Shell"></textarea>';
	        html += '</form>';
            html += '</div>';
            html += '<div id="advanced_submit" style="display:none">advanced</div>';
            html += '<div id="expert_submit" style="display:none"><textarea id="submit_box" style="min-height:400px">'
            html += '</textarea></div>';

            if (data.dataset == null) {
                html += '<textarea id="description" placeholder="Description"></textarea>';
                html += '<div>Number of jobs: <input id="number_jobs" value="1" type="number" min="1", step="1"/> <select id="gridspec" style="margin-left:10px">';
                html += '<option selected="selected" value="">ALL</option>';
                for (var g in args.grids) {
                    html += '<option value="'+g+'">'+args.grids[g]['description']+'</option>';
                }
                html += '</select>';
                html += '<button id="submit_action">Submit</button></div>';
            } else {
                html += '<textarea id="description" placeholder="Description">'+data.dataset.description +'</textarea>';
                html += '<span class = "submit_text">Grids: '+data.dataset.gridspec+'</span>';
                //html += '<h4>Description</h4><textarea id="description" style="width:85%;margin-left:1em;min-height:2em">';
                html += '</textarea>';
                if (data.edit) {
                    html += '<button id="submit_action">Update</button>';
                }
            }
            html += '</div>';


            $(data.element).html(html);
            $('#submit_box').val(pprint_json(data.submit_data));

            $('#submit_action').on('click',function(){
                if (data.state == 'basic')
                {
                    var module = {"src": $('#script_url').val().trim(),
                                  "args": $('#arguments').val().trim(),
                                  "env_shell": $('#env_shell').val().trim(),
                                  "env_clear": true};

                    var data1 = [];
                    var add_data = function(t, type){
                        var input = t.split(',');
                        for (var i = 0; i < input.length; i++)
                        {
                            var d = input[i].trim();
                            if (d.length > 0)
                                data1.push({"movement": type, "remote": d,
                                            "compression": false, "type": "permanent"});
                        }
                    };
                    add_data($('#data_input').val(), 'input');
                    add_data($('#data_output').val(), 'output');

                    var tray = {"modules": [module], "data": data1};
                    var job = {"tasks": [ {"trays": [tray] } ] };

                    console.log(JSON.stringify(job));
                    data.submit_data = job;
                }
                else if (data.state == 'expert')
                {
                    var text = $('#expert_submit').find('textarea').off().val();
                    try {
                        data.submit_data = JSON.parse(text);
                    } catch (e) {
                        try {
                            jsonlint.parse(text);
                             $('#error').text('Failed to parse json: ' + e);
                        } catch(e) {
                             $('#error').html('<pre>' + e + '</pre>');
                        }


                        return;
                    }
                }
                if (data.dataset == null) {
                    var njobs = parseInt($('#number_jobs').val());
                    if ( njobs == null || njobs == undefined || isNaN(njobs)) {
                        $('#error').text('Must specify integer number of jobs');
                        return;
                    }
                    private_methods.submit( njobs, $('#gridspec').val(), $('#description').val() );
                } else if (data.edit)
                    private_methods.update($('#description').val());
            });
            var show_unique = function(tab_name){
                var tabs = ['#basic_submit', '#expert_submit', '#advanced_submit'];
                var buttons = ['#basic_button', '#expert_button', '#advanced_button'];
                for(var i = 0; i < tabs.length; i++)
                    if (tabs[i] == tab_name) 
                    {
                        $(tabs[i]).show();
                        $(buttons[i]).attr('class', 'active');
                    }
                    else 
                    {
                        $(tabs[i]).hide();
                        $(buttons[i]).attr('class', '');
                    }
            };
            var goto_basic = function(){
                if (data.state != 'basic') {
                    data.state = 'basic';

                    show_unique('#basic_submit');
                }
            };
            var goto_advanced = function(){
                if (data.state != 'advanced') {
                    data.state = 'advanced';
                    private_methods.build_advanced();
                    show_unique('#advanced_submit');
                }
            };
            var goto_expert = function(){
                if (data.state != 'expert') {
                    data.state = 'expert';
                    private_methods.clean_json();
                    $('#expert_submit').find('textarea')
                    .on('blur',function(){data.submit_data = JSON.parse($(this).off().val());})
                    .val(pprint_json(data.submit_data));
                    show_unique('#expert_submit');
                }
            };
            show_unique('#' + data.state + '_submit');
            $('#basic_button').on('click',goto_basic);
            $('#advanced_button').on('click',goto_advanced);
            $('#expert_button').on('click',goto_expert);
        }
    };

    return function(m,args) {
        if (m == null || m == undefined) {
            throw new Error('must supply (method,arguments)');
        } else if (m in public_methods) {
            public_methods[m](args);
        } else {
            throw new Error('Cannot find method '+m);
        }
    };
})(jQuery);
