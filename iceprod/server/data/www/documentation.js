// Load file from server, using url
function load_xml(url, callback)
{
    var xmlhttp=new XMLHttpRequest();
    xmlhttp.onreadystatechange=function(){
        if (xmlhttp.readyState==4 && xmlhttp.status==200)
            callback(xmlhttp.responseText);
    };
    xmlhttp.open("GET",url,true);
    xmlhttp.send();
}
// url of current documentation page
var page_url = "";

function close_popup()
{
    var overlay = document.getElementById("overlay");
    var popup = document.getElementById("popup");
    overlay.style.display = "none";
    popup.style.display = "none";
    page_url = "";
    window.location.hash = "";
}

// Display documentation page specified by url
function show_doc(url)
{
    console.log(":"+url);
    if (page_url == url) return;
    page_url = url;
    
    if (url == "") // If there is no doc page, close the popup.
    {
        close_popup();
        return;
    }
    
    var overlay = document.getElementById("overlay");
    var popup = document.getElementById("popup");
    
    // Clicking outside the popup closes the popup
    overlay.onclick = close_popup;
    
    overlay.style.display = "block";
    popup.style.display = "block";
    
    window.location = "#" + url;
    popup.innerHTML = "Loading...";
    load_xml("docs/" + url, function(text){
             popup.innerHTML = text;
             });
    
}
// When the history changes we might need to reload documentation page.
window.onpopstate = function(event) {
    show_doc(window.location.hash.substring(1));
    
}
function load_doc()
{
    show_doc(window.location.hash.substring(1));
}