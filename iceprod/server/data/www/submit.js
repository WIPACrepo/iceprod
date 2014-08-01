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
    
    var private_methods = {
        submit : function(num_jobs, gridspec) {
            RPCclient('submit_dataset',{passkey:data.passkey,data:data.submit_data,njobs:num_jobs,gridspec:gridspec},callback=function(return_data){
                $('#error').html('success');
            });
        },
        build_basic : function( ) {
            var editing = (function(){
                var keys = {};
                var methods = {
                    is: function(key,path) {
                        if (path != null && path != undefined && path != '')
                            key = path+'.'+key;
                        if (key in keys)
                            return keys[key];
                        return keys[key] = false;
                    },
                    set: function(key,val,path) {
                        if (path != null && path != undefined && path != '')
                            key = path+'.'+key;
                        keys[key] = val;
                    }
                };
                return methods;
            })();
            $.views.helpers({
                editable: editing.is,
                isArray: function(input){
                    return $.type(input) === 'array';
                },
                isNotPrivate: function(input){
                    return input.charAt(0) != '_';
                },
                concat: function(a,b,c){
                    if (a != undefined && a != null && a != '') {
                        if (b != undefined) {
                            if (c != undefined && c != null && c != '')
                                ret = a+'.'+c+'_'+b;
                            else
                                ret = a+'.'+b;
                        } else
                            ret = a;
                    } else if (b != undefined)
                        ret = c+'_'+b;
                    else
                        ret = c;
                    return ret;
                }
            });
            $.views.converters({
                intToStr: function(value) {
                    return "" + value;
                },
                strToInt: function(value) {
                    return parseInt(value);
                },
                keyToHtml: function(value) {
                    return "" + value.replace('_',' ');
                },
                singular: function(value) {
                    var ret = "" + value;
                    if (ret.charAt(ret.length-1) == 's')
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
            .on('click','.editable',function(){
                var element = $.view(this).data, parent = $(this).parent(),
                    key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text();
                editing.set(key,true,path);
                $.view(id, true, 'data').refresh();
                window.setTimeout(function(){
                    var input = $(id).find('.editing input');
                    input.focus();
                    var strLength = input.val().length;
                    input[0].setSelectionRange(strLength,strLength);
                },100);
            })
            .on('blur','.editing',function(){
                var parent = $(this).parent(), key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text();
                editing.set(key,false,path);
                $.view(id, true, 'data').refresh();
            })
            .on('click','.add',function(){
                var parent = $(this).parent(), key = $(this).find('span.key').text().replace(' ','_'),
                    path = $(parent).children('span.path').text(),
                    depths = path.split('.'), d = data.submit_data,
                    c = 0, part = [], p = '';
                //console.log('add('+key+','+path+','+JSON.stringify(depths)+')');
                if (path != null && path != undefined && path != '') {
                    for (var i=0;i<depths.length;i++) {
                        part = depths[i].split('_');
                        p = part.slice(0,-1).join('_');
                        c = parseInt(part.slice(part.length-1)[0],10);
                        d = d[p][c];
                    }
                }
                $.observable(d[key+'s']).insert(private_methods.insert_dataclass(d,key);
            });
        },
        new_dataclass : function( e ) {
            if (!(e in dataclasses))
                return undefined;
            var target = dataclasses[e], ret = {'_type':e};
            for (var k in target) {
                if ($.type(target[k]) === 'array')
                    ret[k] = target[k][0];
                else
                    ret[k] = target[k];
            }
            return ret;
        },
        insert_dataclass : function( e, k ) {
            var target = {};
            if ($.type(e) === 'string')
                target = dataclasses[e];
            else if ('_type' in e)
                target = e['_type'];
            if (k in target && $.type(target[k]) === 'array') {
                var ret = target[k][1], t = $.type(ret);
                if (t === 'string' && ret in dataclasses)
                    return private_methods.new_dataclass(ret);
                else if (t === 'object')
                    return $.extend(true,{},ret);
                else
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
            data.state = 'expert';
            data.passkey = args.passkey;
            data.element = $(args.element);
            data.submit_data = private_methods.new_dataclass('Job')
            
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
                    $('#expert_submit').find('textarea')
                    .on('blur',function(){data.submit_data = $(this).off().val();})
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