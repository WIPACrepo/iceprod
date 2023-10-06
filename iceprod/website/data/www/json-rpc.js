/*!
 * a json-rpc client
 */

var RPCclient = (function($){
    var id = 0;
    $.ajaxPrefilter('json',function(options) {
        options.data = JSON.stringify(options.data);
    });
    return function(method,params,callback) {
        var lid = id++;
        var data = {'jsonrpc':'2.0','method':method,'id':lid}
        if (params != null && params != undefined) {
            data['params'] = params;
        }
        $.ajax({url:'/jsonrpc',
                data:data,
                type:'POST',
                contentType:'application/json',
                dataType:'json',
                processData:false,
                cache:false,
                error:function(xhr,error,errorThrown){
                   $('#error').text('error: '+error+" "+errorThrown)
                },
                success:function(ret){
                    if ('error' in ret) {
                        $('#error').text(JSON.stringify(ret['error']));
                    } else if (!('jsonrpc' in ret) || 
                               0+ret['jsonrpc'] != 2 ||
                               !('result' in ret) ||
                               !('id' in ret) ||
                               0+ret['id'] != lid) {
                        $('#error').text('error: json result not formatted correctly');
                    } else if (callback != null && callback != undefined) {
                        callback(ret['result'])
                    }
                }
        });
    }
})(jQuery);
