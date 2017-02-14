/*!
 * Util functions
 */

var convertToAssociative = function( arr ) {
    var len = arr.length;
    var newarr = {};
    for (var i=0;i<len;i++) {
        if (arr[i] != undefined) { newarr[arr[i]] = arr[i]; }
    }
    return newarr;
};

var isAssociativeEmpty = function( obj ) {
    var name;
    for (name in obj) {
        if (obj.hasOwnProperty(name)) {
            return false;
        }
    }
    return true;
};


var isInt = function( val ) {
    return /^[\-0-9]+$/.test(val);
}

if (!String.prototype.startsWith) {
    String.prototype.startsWith = function(searchString, position){
      position = position || 0;
      return this.substr(position, searchString.length) === searchString;
  };
}

function stringify(v, indent)
{
    var s = '';
    if (typeof v == 'object')
    {
        if (v === null)
            s += v;
        else if (v.constructor == Array)
        {
            if (v.length == 0) s += '[]';
            else
            {
                s += '[\n';
                for (var i = 0; i < v.length; i++)
                {
                    var ii = indent + '  ';
                    s += ii + stringify(v[i], ii);
                    if (i + 1 < v.length) s += ',\n';
                    else s += '\n';
                }
                s += indent + ']';
            }
        }
        else
        {
            var s = '{\n';
            var keys = Object.keys(v);
            keys.sort(function (a, b) {
                return a.toLowerCase().localeCompare(b.toLowerCase());
            });
            for (var i = 0; i < keys.length; i++)
            {
                var k = keys[i];
                if (k.startsWith('jQuery')) continue; // HACK removes trash
                s += indent + '  "' + k +'": ';

                var v2 = v[k];
                s += stringify(v2, indent + '  ');

                if (i + 1 < keys.length) s += ',\n';
                else s += '\n';
            }
            s +=  indent + '}';
        }
    }
    else
    {
        if (typeof v == 'string') s += '"' + v.split('"').join('\\"') + '"';
        //else if (v instanceof Function) return s;
        else s += v;
    }
    return s;
}

var pprint_json = function(obj) {

    return stringify(obj, '');
/*
    var ret = '', parts = JSON.stringify(obj,null,'\n').split('\n'), prev_part = '',part='';
    for (var i=0;i<parts.length;i++) {
        part = parts[i]
        if (part == '')
            if (prev_part in {'[':1,'{':1,']':1})
                part = prev_part;
            else if (prev_part == '')
                ret += '  ';
            else
                ret += '\n';
        else
            ret += part;
        prev_part = part;
    }
    return ret;
*/
};

var popup = (function( $ ){
    var state = null;

    var privateMethods = {
        ok : function( ) {
            var options = $(this).data('options');
            if (options.ok != null) {
                if (options.innerhtml != null) {
                    options.ok();
                } else {
                    options.ok($(this).find('input[type=text]').val());
                }
            }
            $(this).fadeOut(function(){$(this).remove();});
            state = null;
            return false;
        },
        cancel : function( ) {
            var options = $(this).data('options');
            if (options.cancel != null) {
                if (options.innerhtml != null) {
                    options.cancel();
                } else {
                    options.cancel($(this).find('input[type=text]').val());
                }
            }
            $(this).fadeOut(function(){$(this).remove();});
            state = null;
            return false;
        },
    };

    var publicMethods = {
        ok : function ( fun ) {
            // set ok event handler
            var options = $(this).data('options');
            if (fun != undefined && fun != null) {
                options.ok = fun;
            } else {
                options.ok = null;
            }
        },
        cancel : function ( fun ) {
            // set cancel event handler
            var options = $(this).data('options');
            if (fun != undefined && fun != null) {
                options.cancel = fun;
            } else {
                options.cancel = null;
            }
        },
        getObj : function ( ) {
            return $(this).find('div.inner');
        }
    };

    var init = function( options ) {
        options = $.extend({
            ok : null,
            cancel : null,
            title : 'Edit:',
            button : 'Save',
            value : '',
            innerhtml : null,
            height : null,
            width : null,
        },options);


        if (state != null) {
            $('.popup_makespreadsheets').fadeOut(function(){$(this).remove();});
            state = null;
        }

        $('body').append('<div class="popup_makespreadsheets"><div class="inner"></div></div>');
        var outerpopup = $('div.popup_makespreadsheets').filter(':last');
        $(outerpopup).data('options',options);
        var popup = $(outerpopup).find('div.inner');

        var marginheight = null;
        var marginwidth = null;
        if (options.height != null) {
            if (options.height == 'auto') {
                marginheight = 10;
            } else {
                marginheight = parseFloat(options.height.slice(0,-2),10)/2;
            }
            $(popup).css('height',options.height);
        }
        if (options.width != null) {
            if (options.width != 'auto') {
                marginwidth = parseFloat(options.width.slice(0,-2),10)/2;
            }
            $(popup).css('width',options.width);
        }
        if (marginwidth != null || marginheight != null) {
            if (marginwidth == null) { marginwidth = 12.5; }
            if (marginheight == null) { marginheight = 3.5; }
            $(popup).css('margin','-'+marginheight+'em 0 0 -'+marginwidth+'em');
        }

        if (options.innerhtml != null) {
            $(popup).append('<p>'+options.title+'</p>'+options.innerhtml+'<button>'+options.button+'</button>');
        } else {
            $(popup).append('<p>'+options.title+'</p><input type="text" /><button>'+options.button+'</button>');
            var input = $(popup).find('input[type=text]');
            input.val(options.value);
            $(input).css('margin-right','.5em');
        }
        var button = $(popup).find('button');

        $(popup).on('keydown','input',function(e){
            var code = (e.keyCode ? e.keyCode : e.which);
            if (code == 13) {
                $(button).trigger('click');
                return false;
            }
        });

        $(button).on('click',function(){privateMethods.ok.apply(outerpopup);});
        $(button).button();
        $(popup).on('click',function(){return false;});
        $(outerpopup).on('click',function(){privateMethods.cancel.apply(outerpopup);});

        $(popup).show();
        state = true;

        return $(outerpopup);
    };

    return function( options ){
        var obj = init(options);

        return function( method ) {
            if ( publicMethods[method] ) {
                return publicMethods[method].apply( obj, Array.prototype.slice.call( arguments, 1 ));
            } else {
                $.error( 'Method ' +  method + ' does not exist on popup' );
            }
        };
    };
})( jQuery );
