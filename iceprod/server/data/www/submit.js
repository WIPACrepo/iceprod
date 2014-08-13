/*!
 * Dataset submission tools
 */

var Submission = (function( $ ) {

    var data = {
        element : null,
        state : 'expert',
        passkey : null,
        submit_data : {}
    };
    
    function pluralize(value) {
        var ret = "" + value;
        if (ret == 'DifPlus')
            return 'difplus';
        else if (ret == 'steering')
            return 'steering';
        else if (ret == 'batchsys')
            return 'batchsys';
        else if (ret.charAt(ret.length-1) == 'y')
            return ret.slice(0,-1)+'ies';
        else
            return ret+'s';
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
                for (var i in json) {
                    if (i == '_type')
                        continue;
                    json[i] = private_methods.clean_json(json[i]);
                }
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
                if (t in dataclasses) {
                    var parent = dataclasses[t];
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
        submit : function(num_jobs, gridspec) {
            private_methods.clean_json();
            RPCclient('submit_dataset',{passkey:data.passkey,data:data.submit_data,njobs:num_jobs,gridspec:gridspec},callback=function(return_data){
                $('#error').html('success');
            });
        },
        build_basic : function( ) {
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
                        if (!(path === undefined) && path != null && path != '')
                            key = path+'.'+key;
                        console.log('editing.set() key='+key);
                        keys[key] = val;
                    },
                    clearall: function() {
                        keys = {};
                    }
                };
                return methods;
            })();
            $.views.helpers({
                editable: editing.is,
                isNull: function(input){
                    return input === null;
                },
                isArray: function(input){
                    return $.type(input) === 'array';
                },
                isObject: function(input){
                    return $.type(input) === 'object';
                },
                isNotPrivate: function(input){
                    return input.charAt(0) != '_';
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
                }
            });
            $.views.converters({
                intToStr: function(value) {
                    if (value == null || value == undefined)
                        return ""
                    else
                        return "" + value;
                },
                strToInt: function(value) {
                    if (value != '' && !isNaN(value)) {
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
                singular: function(value) {
                    var ret = "" + value;
                    if (ret == 'difplus')
                        return 'DifPlus';
                    else if (ret == 'batchsys')
                        return 'batchsys';
                    else if (ret.slice(-3) == "ies")
                        return ret.slice(0,-3)+"y";
                    else if (ret.charAt(ret.length-1) == 's')
                        return ret.slice(0,-1);
                    else
                        return ret;
                }
            });
            $.templates({
                AdvTmpl:"#AdvTmpl"
            });
            var id = '#basic_submit';
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
                        console.warn('no input for key:'+key);
                    }
                },100);
            })
            .on('blur','.editing',function(){
                var parent = $(this).parent().parent(), 
                    key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text();
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
                var parent = $(this).parent(), key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text(),
                    depths = path.split('.'), d = data.submit_data,
                    c = 0, part = [], p = '';
                //console.log('add('+key+','+path+','+JSON.stringify(depths)+')');
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
                var pkey = pluralize(key);
                var obj = private_methods.insert_dataclass(d,pkey);
                if ($(this).hasClass('null')) {
                    console.log('add() null');
                    console.log(d[pkey]);
                    d[pkey] = obj;
                    console.log(d[pkey]);
                    $.view(id, true, 'data').refresh();
                } else if ($(this).hasClass('array')) {
                    console.log('add() array')
                    $.observable(d[pkey]).insert(obj);
                } else if ($(this).hasClass('object')) {
                    console.log('add() object')
                    d[pkey]['newkey'] = obj;
                    $.view(id, true, 'data').refresh();
                }
            });
            $('body').on('blur',function(){
                editing.clearall();
            });
        },
        new_dataclass : function( t ) {
            // return a new dataclass of type t
            if (!(t in dataclasses)) {
                console.log(t+' not in dataclasses');
                return undefined;
            }
            console.log('making new dataclass: '+t);
            var target = dataclasses[t], ret = {'_type':t};
            for (var k in target) {
                if ($.type(target[k]) === 'array')
                    ret[k] = target[k][0];
                else
                    ret[k] = target[k];
            }
            console.log(ret);
            return ret;
        },
        insert_dataclass : function( parent, k ) {
            // return a new dataclass for the key in parent
            var target = {};
            if (parent === 'option') {
                console.log('making string')
                return '';
            } else if ($.type(parent) === 'string')
                target = dataclasses[parent];
            else if ('_type' in parent)
                target = dataclasses[parent['_type']];
            if (k in target && $.type(target[k]) === 'array') {
                var ret = target[k][1], t = $.type(ret);
                if (t === 'string' && ret in dataclasses) {
                    return private_methods.new_dataclass(ret);
                } else if (t === 'object') {
                    console.log('making new dict');
                    return $.extend(true,{},ret);
                } else {
                    console.log('making new ret');
                    console.log(ret);
                    return ret;
                }
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
            data.state = 'expert';
            data.passkey = args.passkey;
            data.element = $(args.element);
            data.submit_data = private_methods.new_dataclass('Job')
            private_methods.clean_json();
            
            var html = '<div><button id="basic_button">Basic View</button> <button id="expert_button">Expert View</button></div></div>';
            html += '<div id="basic_submit" style="display:none">basic</div>';
            html += '<div id="expert_submit"><textarea id="submit_box" style="width:90%;min-height:400px">'
            html += '</textarea></div>';
            html += '<div>Number of jobs: <input id="number_jobs" value="1" /> <select id="gridspec" style="margin-left:10px">';
            for (var g in args.gridspec) {
                html += '<option value="'+g+'">'+args.gridspec[g][1]+'</option>';
            }
            html += '</select></div>';
            html += '<button id="submit_action" style="padding:1em;margin:1em">Submit</button>';
            $(data.element).html(html);
            $('#submit_box').val(pprint_json(data.submit_data));
            
            $('#submit_action').on('click',function(){
                var njobs = ValueTypes.int.coerce($('#number_jobs').val());
                if ( njobs == null || njobs == undefined ) {
                    $('#error').text('Must specify integer number of jobs');
                    return;
                }
                private_methods.submit( njobs, $('#gridspec').val() );
            });
            var goto_basic = function(){
                if (data.state != 'basic') {
                    data.state = 'basic';
                    private_methods.build_basic();
                    $('#expert_submit').hide();
                    $('#basic_submit').show();
                }
            };
            var goto_expert = function(){
                if (data.state != 'expert') {
                    data.state = 'expert';
                    //private_methods.clean_json();
                    $('#expert_submit').find('textarea')
                    .on('blur',function(){data.submit_data = JSON.parse($(this).off().val());})
                    .val(pprint_json(data.submit_data));
                    $('#basic_submit').hide();
                    $('#expert_submit').show();
                }
            };
            $('#basic_button').on('click',goto_basic);
            $('#expert_button').on('click',goto_expert);
            
            // default to a view
            if (data.state == 'basic')
                goto_basic();
            else
                goto_expert();
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