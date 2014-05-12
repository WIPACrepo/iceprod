/*!
 * Dataset submission tools
 *
 * Copyright 2013, IceCube Collaboration
 */


var pprint = (function() {
    var indent = 0;
    var get_tag_type = function(tag) {
        var t = 0;
        if (tag.substring(0,2) == '</') { 
            t = -1;
        } else if (tag.substring(tag.length-2) == '/>') {
            t = 0;
        } else {
            t = 1;
        }
        return t;
    };
    var tabber = function(match) {
        var ret = '';
        var t = get_tag_type(match);
        if (t < 0) { indent -= 2; }
        for (var i=0;i<indent;i++) { ret += ' '; }
        if (t > 0) { indent += 2; }
        return ret+match;
    };
    return function(xml) {
        // insert line breaks and tabs into xml source to make it more readable
        xml = xml.replace(/\/>/gi,' />');
        xml = xml.replace(/></gi,'>\n<');
        xml = xml.replace(/(.+)/gi,tabber); // match every line
        return xml;
    };
})();

var xmlToString = function(xmlData) {
    // convert from xml DOM to xml string
    var xmlString = undefined;
    if (window.ActiveXObject) {
        xmlString = xmlData.xml;
    }
    if (xmlString === undefined) {
        var oSerializer = new XMLSerializer();
        xmlString = oSerializer.serializeToString(xmlData);
    }
    return xmlString;
};

var XMLtoHTMLelement = (function( $ ) {
    // convert from an XML element to an HTML div element
    return function converter(xml) {
        var e = $('<div class="xml xml_'+xml.tagName.toLowerCase()+'"></div>');
        var data = {};
        if (xml.attributes != undefined) {
            for (var i=0;i<xml.attributes.length;i++){
                var attr = xml.attributes[i];
                data[attr.name] = attr.value;
            }
        }
        e.data('data',{type:xml.tagName.toLowerCase(),attrs:data});
        $(xml).children().each(function(){
            $(e).append(converter(this));
        });
        return e;
    };
})(jQuery);

var HTMLtoXMLelement = (function( $ ) {
    // convert from an HTML div element to an XML element
    var xmlDocument = $.parseXML('<root/>');
    return function converter(e) {
        if (e.tagName.toLowerCase() != 'div' || 
            !($(e).hasClass('xml') || $(e).hasClass('xml_basic') || $(e).hasClass('xml_basic_line'))) {
            return null;
        }
        var data = $(e).data('data');
        var type = data.type;
        var attrs = data.attrs;
        var xml = xmlDocument.createElement(type);
        for (a in attrs) {
            $(xml).attr(a,attrs[a])
        }
        $(e).children('div.xml,div.xml_basic,div.xml_basic_section,div.xml_basic_line').each(function(){
            if ($(this).hasClass('xml_basic_section')) {
                $(this).children('div.xml_basic_line').each(function(){
                    var x = converter(this);
                    if (x != undefined && x != null) {
                        $(xml).append(x);
                    }
                });
            } else {
                var x = converter(this);
                if (x != undefined && x != null) {
                    $(xml).append(x);
                }
            }
        });
        return xml;
    };
})(jQuery);


var Submission = (function( $ ) {

    var xml_doctype = '<?xml version="1.0" encoding="UTF-8"?>\n';
    xml_doctype += '<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "env/share/iceprod/iceprod.v3.dtd">\n';

    var data = {
        advanced : false,
        element : null,
        state : 'expert'
    };
    
    // define all the xml objects
    var makeHTMLinput = function(e){ var x = $('<input type="text" />'); $(x).val(e); return x; };
    var getHTMLinput = function(e){ return $(e).val(); };
    var ValueTypes = {
        "bool" : { makeHTML : function(e){
                                  var x = $('<input type="checkbox" />');
                                  $(x).prop("checked",e);
                                  return x;
                              },
                  getValue : function(e){ return $(e).prop("checked"); },
                  coerce : function(v) {
                               if ($.type(v) === 'string') { return (v.toLowerCase() == 'true'); }
                               else { return (v == true); }
                           },
                  validate : function(e){ return (e == true || e == false); },
                  print : "bool"
                 },
        "int" : { makeHTML : makeHTMLinput,
                  getValue : function(e){ return ValueTypes.int.coerce(getHTMLinput(e)); },
                  coerce : function(v) {
                               if (ValueTypes.int.validate(v)) { return parseInt(v) }
                               else { return null; }
                           },
                  validate : function(e){ return isInt(e); },
                  print : "int"
                },
        "float" : { makeHTML : makeHTMLinput,
                    getValue : function(e){ return ValueTypes.float.coerce(getHTMLinput(e)); },
                    coerce : function(v) {
                                 if (ValueTypes.float.validate(v)) { return parseFloat(v) }
                                 else { return null; }
                             },
                    validate : function(e){ return $.isNumeric(e); },
                    print : "float"
                  },
        "str" : { makeHTML : makeHTMLinput,
                  getValue : getHTMLinput,
                  coerce : function(v) { return ''+v; },
                  validate : function(e){ return (e != null && e != undefined); },
                  print : "str"
                },
        "text" : { makeHTML : function(e){ x = $('<textarea />'); $(x).val(e); return x; },
                   getValue : function(e){ return $(e).val(); },
                   coerce : function(v) { return ''+v; },
                   validate : function(e){ return (e != null && e != undefined); },
                   print : "text"
                 }
    };
    var XMLObjects = {
        configuration : {attribs : {"xmlns:xsi" : [false,'http://www.w3.org/2001/XMLSchema-instance',true],
                                    "xsi:noNamespaceSchemaLocation" : [false,'config.xsd',true],
                                    iceprod_version : [ValueTypes.float,2.0,true],
                                    version : [ValueTypes.float,3.0,true],
                                    parentid : [ValueTypes.str,'',true]
                                   },
                         attribs_order : ["iceprod_version","version","parentid"],
                         subtags : {steering : [false,1], // not required, max of 1
                                    task : [true,null] // required, no maximum
                                   }
                        },
        steering : {attribs : {},
                    attribs_order : [],
                    subtags : {parameter : [false, null],
                               system : [false, 1],
                               batchsys : [false, 1],
                               resource : [false, null],
                               data : [false, null]
                              }
                   },
        task : {attribs : {name : [ValueTypes.str,'',false],
                           depends : [ValueTypes.str,'',false]
                          },
                attribs_order : ["name","depends"],
                subtags : {tray : [true, null],
                           batchsys : [false, 1],
                           parameter : [false, null],
                           resource : [false, null],
                           data : [false, null],
                           "class" : [false, null],
                           project : [false, null]
                          }
               },
        tray : {attribs : {name : [ValueTypes.str,'',false],
                           iter : [ValueTypes.int,1,false]
                          },
                attribs_order : ["name","iter"],
                subtags : {module : [true, null],
                           parameter : [false, null],
                           resource : [false, null],
                           data : [false, null],
                           "class" : [false, null],
                           project : [false, null]
                          }
               },
        module : {attribs : {name : [ValueTypes.str,'',false],
                             "class" : [ValueTypes.str,'',false],
                             src : [ValueTypes.str,'',false],
                             args : [ValueTypes.text,'',false]
                            },
                  attribs_order : ["name","class","src","args"],
                  subtags : {parameter : [false, null],
                             resource : [false, null],
                             data : [false, null],
                             "class" : [false, null],
                             project : [false, null]
                            }
                 },
        parameter : {attribs : {name : [ValueTypes.str,'',true],
                                value : [ValueTypes.str,'',false],
                                type : [ValueTypes.str,'',false]
                               },
                     attribs_order : ["name","value","type"],
                     subtags : {}
                    },
        batchsys : {attribs : {name : [ValueTypes.str,'',false]},
                    attribs_order : ["name"],
                    subtags : {parameter : [false, null]}
                   },
        system : {attribs : {},
                  attribs_order : [],
                  subtags : {parameter : [false, null]},
                  readonly : true
                 },
        "class" : {attribs : {name : [ValueTypes.str,'',true],
                              src : [ValueTypes.str,'',false],
                              recursive : [ValueTypes.bool,false,false],
                              resource_name : [ValueTypes.str,'',false],
                              libs : [ValueTypes.str,'',false],
                              env_vars : [ValueTypes.text,'',false]
                             },
                   attribs_order : ["name","src","recursive","resource_name","libs","env_vars"],
                   subtags : {}
                  },
        project : {attribs : {name : [ValueTypes.str,'',false],
                              "class" : [ValueTypes.str,'',true]
                             },
                   attribs_order : ["name","class"],
                   subtags : {}
                  },
        resource : {attribs : {remote : [ValueTypes.str,'',false],
                               local : [ValueTypes.str,'',false],
                               compression : [ValueTypes.bool,false,false]
                              },
                    attribs_order : ["remote","local","compression"],
                    subtags : {}
                   },
        data : {attribs : {remote : [ValueTypes.str,'',false],
                           local : [ValueTypes.str,'',false],
                           compression : [ValueTypes.bool,false,false],
                           type : [ValueTypes.str,'permanent',true],
                           movement : [ValueTypes.str,'',true]
                          },
                attribs_order : ["remote","local","compression","type","movement"],
                subtags : {}
               }
    }
    
    var helper_methods = {
        AddHelpers : function(e) {
            if (e == undefined || e == null) { return; }
            var data = $(e).data('data');
            var tag = data.type;
            if (!(tag in XMLObjects)) {
                // tag not found
                return;
            }
            var obj = XMLObjects[tag];
            var attr_obj = $('<div class="xml_attribs"/>');
            for (var i=obj.attribs_order.length-1;i>=0;i--) {
                var attr = obj.attribs_order[i];
                var a = obj.attribs[attr];
                if (a[0] != false) {
                    var v = a[1];
                    var c = ' class="xml_attr xml_attr_'+a[0].print+' xml_attr_'+tag+'_'+attr+'"';
                    if (attr in data.attrs) {
                        var vnew = a[0].coerce(data.attrs[attr]);
                        if (vnew != null && vnew != undefined && a[0].validate(vnew)) {
                            v = vnew;
                        }
                    }
                    var newe = $('<div'+c+'><span class="xml_attr_label">'+attr+'</span></div>');
                    $(newe).append(a[0].makeHTML(v));
                    if (a[0] == ValueTypes.text) {
                        $(attr_obj).append(newe);
                    } else {
                        $(attr_obj).prepend(newe);
                    }
                }
            }
            var subtags_obj = $('<div class="xml_subtags"/>');
            if (!('readonly' in obj) || obj.readonly != true) {
                for (var subt in obj.subtags) {
                    var t = obj.subtags[subt];
                    if (t[1] == null) {
                        // add button to optionally add more
                        var newe = $('<button>Add '+subt+'</button>');
                        newe.data('tag',subt);
                        $(subtags_obj).append(newe);
                    } else if (t[1] == 1 && $(e).children('div.xml_'+subt).length < 1) {
                        // just add it by default
                        var newe = $('<'+subt+'/>');
                        for (var a in XMLObjects[subt].attribs) {
                            $(newe).attr(a,XMLObjects[subt].attribs[a][1])
                        }
                        $(e).prepend(XMLtoHTMLelement(newe[0]));
                    }
                }
            }
            if ($(attr_obj).children().length > 0) {
                $(e).prepend(attr_obj);
            }
            if ($(subtags_obj).children().length > 0) {
                $(e).append(subtags_obj);
            }
            $(e).prepend('<h4 class="tag_title">'+tag+'</h4>');
            $(e).children('div.xml').each(function(){ helper_methods.AddHelpers(this); });
        },
        RemoveHelpers : function(e) {
            if (e == undefined || e == null) { return; }
            $(e).children('div.xml').each(function(){ helper_methods.RemoveHelpers(this); });
            var data = $(e).data('data');
            var tag = data.type;
            if (!(tag in XMLObjects)) {
                // tag not found
                return;
            }
            var obj = XMLObjects[tag];
            // get all attributes
            for (var attr in obj.attribs) {
                var a = obj.attribs[attr];
                if (a[0] != false) {
                    var v = a[1];
                    var attr_obj = $(e).find('div.xml_attribs div.xml_attr_'+tag+'_'+attr)[0];
                    var attr_val = $(attr_obj).children('input,textarea')[0];
                    var newv = a[0].getValue(attr_val);
                    if (a[0].validate(newv)) {
                        data.attrs[attr] = newv;
                    }
                } else {
                    // do this to fix any stray editing in the advanced view
                    data.attrs[attr] = a[1];
                }
                if (a[2] == false && data.attrs[attr] == a[1]) {
                    // this can be safely removed
                    delete data.attrs[attr];
                }
            }
            // check if we need to keep this element
            if ($(e).children('div.xml').length < 1) {
                // there are no subtags
                var parent_tag = $(e).parent().data('data').type;
                if (parent_tag in XMLObjects && 
                    tag in XMLObjects[parent_tag].subtags &&
                    XMLObjects[parent_tag].subtags[tag][0] == false) {
                    // this element is optional, check for changes in attrs
                    var at_defaults = true;
                    for (var attr in obj.attribs) {
                        var a = obj.attribs[attr];
                        if (a[0] != false && data.attrs[attr] != undefined && data.attrs[attr] != a[1]) {
                            at_defaults = false;
                            break;
                        }
                    }
                    if (at_defaults == true) {
                        // element is at defaults and is optional, so remove it
                        $(e).remove();
                        return;
                    }
                }
            }
        },
        AddTag : function(event) {
            var button = $(this);
            var tag = $(button).data('tag');
            if (!(tag in XMLObjects)) {
                // tag not found
                return false;
            }
            var obj = XMLObjects[tag];
            // add obj to the parent
            var newe = $('<'+tag+'/>');
            for (var a in XMLObjects[tag].attribs) {
                $(newe).attr(a,XMLObjects[tag].attribs[a][1])
            }
            newe = XMLtoHTMLelement(newe[0]);
            helper_methods.AddHelpers(newe);
            $(button).parent().before(newe);
        }
    };
    
    var private_methods = {
        submit : function(submit_data, num_jobs, gridspec) {
            RPCclient('submit_dataset',{passkey:data.passkey,xml:submit_data,njobs:num_jobs,gridspec:gridspec},callback=function(return_data){
                $('#error').html('success');
            });
        },
        build_basic : function( ) {
            // translate from xml to DOM elements
            var xmlDoc = $.parseXML($('#submit_box').val());
            var task = $(xmlDoc.documentElement).find('task');
            // test if the basic view can handle this config
            if ($(xmlDoc.documentElement).find('steering').length > 0 ||
                $(task).length > 1 || $(task).children('tray').length > 1 ||
                $(task).children(':not(tray)').length > 0 ||
                $(task).find('project').length > 0) {
                return false;
            }
            var config = XMLtoHTMLelement(xmlDoc.documentElement);
            var config_out = $('<div class="xml_basic xml_basic_config"/>');
            $(config_out).data('data',$(config).data('data'));
            task = $(config).children('div.xml_task:first');
            var task_out = $('<div class="xml_basic xml_basic_task"/>');
            $(task_out).data('data',$(task).data('data'));
            var tray = $(task).children('div.xml_tray:first');
            var tray_out = $('<div class="xml_basic xml_basic_tray"/>');
            $(tray_out).data('data',$(tray).data('data'));
            var input_section = $('<div class="xml_basic_section xml_basic_input"><h4>Input</h4></div>');
            var class_section = $('<div class="xml_basic_section xml_basic_class"><h4>Classes</h4></div>');
            var mod_section = $('<div class="xml_basic_section xml_basic_mod"><h4>Modules</h4></div>');
            var params_section = $('<div class="xml_basic_section xml_basic_params"><h4>Parameters</h4></div>');
            var output_section = $('<div class="xml_basic_section xml_basic_output"><h4>Output</h4></div>');
            
            var getObj = function(type,data){
                var obj = {};
                for (var attr in type.attribs) {
                    obj[attr] = type.attribs[attr][1];
                    if (data == undefined || data.attrs[attr] == undefined) { continue; }
                    var vnew = type.attribs[attr][0].coerce(data.attrs[attr]);
                    if (vnew != null && vnew != undefined && type.attribs[attr][0].validate(vnew)) {
                        obj[attr] = vnew;
                    }
                }
                return obj;
            }
            
            // get data/resources
            input_section.append('<div class="xml_basic_header"><div class="xml_basic_remote">Source</div><div class="xml_basic_local">Local</div></div>');
            output_section.append('<div class="xml_basic_header"><div class="xml_basic_local">Local</div><div class="xml_basic_remote">Destination</div></div>');
            var makeData = function (data) {
                var newe = $('<div class="xml_basic_line"/>');
                $(newe).data('data',data);
                if (data.type == 'resource') {
                    var obj = getObj(XMLObjects.resource,data);
                    newe.append('<input class="xml_basic_line_remote" type="text" value="'+obj.remote+'" />');
                    newe.append('<input class="xml_basic_line_local" type="text" value="'+obj.attrs.local+'" />');
                    input_section.append(newe);
                } else {
                    var obj = getObj(XMLObjects.data,data);
                    if (obj.movement == 'input' || obj.movement == 'both') {
                        newe.append('<input class="xml_basic_line_remote" type="text" value="'+obj.remote+'" />');
                        newe.append('<input class="xml_basic_line_local" type="text" value="'+obj.local+'" />');
                        input_section.append(newe);
                    } else {
                        newe.append('<input class="xml_basic_line_local" type="text" value="'+obj.local+'" />');
                        newe.append('<input class="xml_basic_line_remote" type="text" value="'+obj.remote+'" />');
                        output_section.append(newe);
                    }
                }
            };
            $(tray).find('div.xml_data,div.xml_resource').each(function(){
                makeData($(this).data('data'));
            });
            makeData({type:'data',attrs:{movement:'input',type:XMLObjects.data.attribs.type[1]}});
            $(input_section).on('change','input',function(){
                if ($(this).val() == '' && $(this).parent().nextAll().length > 0) {
                    var empty = true;
                    $(this).siblings('input').each(function(){
                        if ($(this).val() != '') { empty = false; }
                    });
                    if (empty == true) {
                        $(this).parent().remove();
                    }
                } else if ($(this).val() != '' && $(this).parent().nextAll().length == 0) {
                    makeData({type:'data',attrs:{movement:'input'}});
                }
            });
            makeData({type:'data',attrs:{movement:'output',type:XMLObjects.data.attribs.type[1]}});
            $(output_section).on('change','input',function(){
                if ($(this).val() == '' && $(this).parent().nextAll().length > 0) {
                    var empty = true;
                    $(this).siblings('input').each(function(){
                        if ($(this).val() != '') { empty = false; }
                    });
                    if (empty == true) {
                        $(this).parent().remove();
                    }
                } else if ($(this).val() != '' && $(this).parent().nextAll().length == 0) {
                    makeData({type:'data',attrs:{movement:'output'}});
                }
            });
            // get class
            class_section.append('<div class="xml_basic_header"><div class="xml_basic_name">Name</div><div class="xml_basic_src">Source</div><div class="xml_basic_recursive">Tarball</div><div class="xml_basic_env_vars">Env Variables</div></div>');
            var makeClass = function (data) {
                var newe = $('<div class="xml_basic_line"/>');
                $(newe).data('data',data);
                var obj = getObj(XMLObjects.class,data);
                newe.append('<input class="xml_basic_line_name" type="text" value="'+obj.name+'" />');
                newe.append('<input class="xml_basic_line_src" type="text" value="'+obj.src+'" />');
                newe.append('<input class="xml_basic_line_recursive" type="checkbox" '+(obj.recursive ? 'checked' : '')+' />');
                newe.append('<input class="xml_basic_line_env_vars" type="text" value="'+obj.env_vars+'" />');
                class_section.append(newe);
            };
            $(tray).find('div.xml_class').each(function(){
                makeClass($(this).data('data'));
            });
            makeClass({type:'class',attrs:{}});
            $(class_section).on('change','input',function(){
                if (($(this).val() == '' || $(this).prop('checked') === false) && $(this).parent().nextAll().length > 0) {
                    var empty = true;
                    $(this).siblings('input[type=text]').each(function(){
                        if ($(this).val() != '') { empty = false; }
                    });
                    $(this).siblings('input[type=checkbox]').each(function(){
                        if ($(this).prop('checked') != false) { empty = false; }
                    });
                    if (empty == true) {
                        $(this).parent().remove();
                    }
                } else if (($(this).prop('type') == 'text' && $(this).val() != '' || $(this).prop('type') == 'checkbox' && $(this).prop('checked') === true) && $(this).parent().nextAll().length == 0) {
                    makeClass({type:'class',attrs:{}});
                }
            });
            // get module
            mod_section.append('<div class="xml_basic_header"><div class="xml_basic_src">Source</div><div class="xml_basic_class">Class</div><div class="xml_basic_args">Arguments</div></div>');
            var makeModule = function (data) {
                var newe = $('<div class="xml_basic_line"/>');
                $(newe).data('data',data);
                var obj = getObj(XMLObjects.module,data);
                newe.append('<input class="xml_basic_line_src" type="text" value="'+obj.src+'" />');
                newe.append('<input class="xml_basic_line_class" type="text" value="'+obj.class+'" />');
                newe.append('<input class="xml_basic_line_args" type="text" value="'+obj.args+'" />');
                mod_section.append(newe);
            };
            $(tray).find('div.xml_module').each(function(){
                makeModule($(this).data('data'));
            });
            var modEmpty = true;
            $(mod_section).find('div.xml_basic_line:last input').each(function(){
                if ($(this).val() != '') { modEmpty = false; }
            });
            if (modEmpty != true) { makeModule({type:'module',attrs:{}}) };
            $(mod_section).on('change','input',function(){
                if ($(this).val() == '' && $(this).parent().nextAll().length > 0) {
                    var empty = true;
                    $(this).siblings('input').each(function(){
                        if ($(this).val() != '') { empty = false; }
                    });
                    if (empty == true) {
                        $(this).parent().remove();
                    }
                } else if ($(this).val() != '' && $(this).parent().nextAll().length == 0) {
                    makeModule({type:'module',attrs:{}});
                }
            });
            // get parameters
            params_section.append('<div class="xml_basic_header"><div class="xml_basic_name">Name</div><div class="xml_basic_value">Value</div><div class="xml_basic_type">Type</div></div>');
            var makeParameter = function (data) {
                var newe = $('<div class="xml_basic_line"/>');
                $(newe).data('data',data);
                var obj = getObj(XMLObjects.parameter,data);
                newe.append('<input class="xml_basic_line_name" type="text" value="'+obj.name+'" />');
                newe.append('<input class="xml_basic_line_value" type="text" value="'+obj.value+'" />');
                newe.append('<input class="xml_basic_line_type" type="text" value="'+obj.type+'" />');
                params_section.append(newe);
            };
            $(tray).find('div.xml_parameter').each(function(){
                makeParameter($(this).data('data'));
            });
            makeParameter({type:'parameter',attrs:{}});
            $(params_section).on('change','input',function(){
                if ($(this).val() == '' && $(this).parent().nextAll().length > 0) {
                    var empty = true;
                    $(this).siblings('input').each(function(){
                        if ($(this).val() != '') { empty = false; }
                    });
                    if (empty == true) {
                        $(this).parent().remove();
                    }
                } else if ($(this).val() != '' && $(this).parent().nextAll().length == 0) {
                    makeParameter({type:'parameter',attrs:{}});
                }
            });
            // add to page
            $(tray_out).append(params_section);
            $(tray_out).append(input_section);
            $(tray_out).append(class_section);
            $(tray_out).append(mod_section);
            $(tray_out).append(output_section);
            $(task_out).append(tray_out);
            $(config_out).append(task_out);
            $('#basic_submit').empty().append(config_out);
            return true;
        },
        write_basic : function( ) {
            // translate from DOM elements to xml
            $('#basic_submit').find('div.xml_basic_section:not(.xml_basic_mod)').children('div.xml_basic_line:last-child').remove();
            if ($('#basic_submit').find('div.xml_basic_section.xml_basic_mod').children('div.xml_basic_line').length > 1) {
                $('#basic_submit').find('div.xml_basic_section.xml_basic_mod').children('div.xml_basic_line:last').remove();
            }
            $('#basic_submit').find('div.xml_basic_line').each(function(){
                var data = $(this).data('data');
                var type = XMLObjects[data.type];
                for (var attr in type.attribs) {
                    var e = $(this).children('input.xml_basic_line_'+attr+':first');
                    if (e.length > 0) {
                        var val = null;
                        var t = $(e).prop('type');
                        if (t == 'text') {
                            val = $(e).val();
                        } else if (t == 'checkbox') {
                            val = $(e).prop('checked');
                        }
                        if (val != undefined && val != null) {
                            data.attrs[attr] = val;
                        }
                    }
                    if (data.attrs[attr] != undefined && type.attribs[attr][2] == false && type.attribs[attr][0].coerce(data.attrs[attr]) == type.attribs[attr][1]) {
                        delete data.attrs[attr];
                    }
                }
            });
            var root = $('#basic_submit').children('div.xml_basic')[0];
            var xml = HTMLtoXMLelement(root);
            $('#submit_box').val(xml_doctype+pprint(xmlToString(xml)));
        },
        build_advanced : function( ) {
            // translate from xml to DOM elements
            var xmlDoc = $.parseXML($('#submit_box').val());
            var root = XMLtoHTMLelement(xmlDoc.documentElement);
            // decorate with helpers
            helper_methods.AddHelpers($(root)[0]);
            $(root).find('div.xml_subtags button').on('click',helper_methods.AddTag);
            $('#advanced_submit').empty().append(root);
        },
        write_advanced : function( ) {
            // translate from DOM elements to xml
            $('#advanced_submit').children('div.xml').each(function(){
                helper_methods.RemoveHelpers(this);
            });
            var root = $('#advanced_submit').children('div.xml')[0];
            var xml = HTMLtoXMLelement(root);
            $('#submit_box').val(xml_doctype+pprint(xmlToString(xml)));
        }
    };
    
    var public_methods = {
        init : function(args) {
            if (args == undefined) {
                throw new Error('must supply args');
                return;
            }
            data.passkey = args.passkey;
            data.element = $(args.element);
            
            var xml_default = '<configuration xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="config.xsd" version="3.0" iceprod_version="2.0" parentid="">';
            xml_default += '<task><tray><module src=""/></tray></task></configuration>';
            
            var html = '<div><button id="basic_button">Basic View</button> <button id="advanced_button">Advanced View</button> <button id="expert_button">Expert View</button></div></div>';
            html += '<div id="basic_submit" style="display:none">basic</div>';
            html += '<div id="advanced_submit" style="display:none">advanced</div>';
            html += '<div id="expert_submit"><textarea id="submit_box" style="width:90%;min-height:400px">'
            html += '</textarea></div>';
            html += '<div>Number of jobs: <input id="number_jobs" value="1" /> <select id="gridspec" style="margin-left:10px">';
            for (var g in args.gridspec) {
                html += '<option value="'+g+'">'+args.gridspec[g][1]+'</option>';
            }
            html += '</select></div>';
            html += '<button id="submit_action" style="padding:1em;margin:1em">Submit</button>';
            $(data.element).html(html);
            $('#submit_box').val(xml_doctype+pprint(xml_default));
            data.state = 'expert';
            
            $('#submit_action').on('click',function(){
                if (data.state == 'basic') { private_methods.write_basic(); }
                else if (data.state == 'advanced') { private_methods.write_advanced(); }
                var njobs = ValueTypes.int.coerce($('#number_jobs').val());
                if ( njobs == null || njobs == undefined ) {
                    $('#error').text('Must specify integer number of jobs');
                    return;
                }
                private_methods.submit( $('#submit_box').val(), njobs, $('#gridspec').val() );
            });
            $('#basic_button').on('click',function(){
                if (data.state != 'basic') {
                    if (data.state == 'advanced') { private_methods.write_advanced(); }
                    if (private_methods.build_basic() != true) {
                        $('#error').text('Configuration is too advanced for the Basic View')
                        return;
                    }
                    data.state = 'basic';
                    $('#expert_submit').hide();
                    $('#advanced_submit').hide();
                    $('#basic_submit').show();
                }
            });
            $('#advanced_button').on('click',function(){
                if (data.state != 'advanced') {
                    if (data.state == 'basic') { private_methods.write_basic(); }
                    data.state = 'advanced';
                    $('#basic_submit').hide();
                    $('#expert_submit').hide();
                    private_methods.build_advanced();
                    $('#advanced_submit').show();
                }
            });
            $('#expert_button').on('click',function(){
                if (data.state != 'expert') {
                    if (data.state == 'basic') { private_methods.write_basic(); }
                    else if (data.state == 'advanced') { private_methods.write_advanced(); }
                    data.state = 'expert';
                    $('#basic_submit').hide();
                    $('#advanced_submit').hide();
                    $('#expert_submit').show();
                }
            });
            
            // default to the basic view
            if (private_methods.build_basic() == true) {
                data.state = 'basic';
                $('#expert_submit').hide();
                $('#advanced_submit').hide();
                $('#basic_submit').show();
            }
        },
        set_xml : function(xml) {
            // strip doctype if it exists
            var xmlDoc = $.parseXML(xml);
            xml = xmlToString(xmlDoc.documentElement);
            // add doctype and pretty print
            $('#submit_box').val(xml_doctype+pprint(xml));
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