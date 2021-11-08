var socket = io.connect();

var renderHlist = true;
var Hlist = Array();
var dataSizes = {};

setInterval(function() {
    socket.emit('get_harvester_list');
}, 10000); 

window.onload = function() {
  socket.emit('get_harvester_list');
};

socket.on('get_harvester_list', function(output) {
    
    Hlist = output['list'];
    
//     dataSizes = output['data_sizes'];
    var time = output['time'];
    
    output['data_sizes'].forEach((x, i) => dataSizes[x[0]] = x[1]);
    
    if (renderHlist) {
        renderHarvesterList();
        document.getElementById("list_time").innerHTML = 'last db chck: '+time;
    };
});

function renderHarvesterList() {
    
    var tbody = '';       
    for (let i = 0; i < Hlist.length; i++) {
        tbody+="<tr id=\"h_"+i+"\">";
        tbody+=renderItemView(i);
        tbody+="</tr>";
    };
    
    tbody+="<thead><tr><td></td><td></td>";
    tbody+="<td><div onclick=\"turnOffRender()\" class=\"input-group input-group-sm\"><input id=\"newname\" type=\"text\" class=\"form-control\" placeholder=\"new name\" aria-label=\"name\" aria-describedby=\"basic-addon1\"></div></td>";
    tbody+="<td><div onclick=\"turnOffRender()\" class=\"input-group input-group-sm\"><input id=\"newsymbol\" type=\"text\" class=\"form-control\" placeholder=\"new symbol\" aria-label=\"symbol\" aria-describedby=\"basic-addon1\"></div></td>";
    tbody+="<td></td>";
    tbody+="<td><button id=\"createnew\" type=\"button\" class=\"btn btn-sm btn-outline-secondary w-100\" onclick=\"createHarvesterItem()\">Create new</button></td>";
    tbody+="</tr></thead>";    

    document.getElementById("tbody").innerHTML = tbody;
};

function renderItemView(i) {
    var tbody = '';
    var hid = Hlist[i][0];
    var dskeys = Object.keys(dataSizes);
    
    tbody+="<td><div class=\"form-check form-switch\">";
    tbody+="<input class=\"form-check-input\" type=\"checkbox\" disabled";
    tbody+=(Hlist[i][3]==1)?" checked>":">";
    tbody+="</div></td>";
    tbody+="<td>"+Hlist[i][0]+"</td>";
    tbody+="<td>"+Hlist[i][1]+"</td>";
    tbody+="<td>"+Hlist[i][2]+"</td>";
    tbody+="<td>"+((dskeys.includes(String(hid)))?dataSizes[hid]:0)+"</td>";
    tbody+="<td ><div class=\"btn-group w-100\"><button id=\"editbtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" onclick=\"renderItemEdit("+i+")\">Edit</button>";
    tbody+="<button id=\"updatebtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" disabled>Update</button>";
    tbody+="<button id=\"removebtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" disabled>";
    tbody+="Remove</button></div></td>";
    
    return tbody;
};

function renderItemEdit(i) {
    console.log('edit:',i);

    renderHlist = false;
    socket.emit('get_harvester_list');

    var tbody = '';

    tbody+="<td><div class=\"form-check form-switch\">";
    tbody+="<input id=\"active_"+i+"\" class=\"form-check-input\" type=\"checkbox\"";
    tbody+=(Hlist[i][3]==1)?" checked>":">";
    tbody+="</div></td>";
    tbody+="<td><div id=\"id_"+i+"\">"+Hlist[i][0]+"</div></td>";
    tbody+="<td><div class=\"input-group input-group-sm\"><input id=\"name_"+i+"\" type=\"text\" class=\"form-control\" value=\""+Hlist[i][1]+"\" aria-label=\"name\" aria-describedby=\"basic-addon1\"></div></td>";
    tbody+="<td><div class=\"input-group input-group-sm\"><input id=\"symbol_"+i+"\" type=\"text\" class=\"form-control\" value=\""+Hlist[i][2]+"\" aria-label=\"symbol\" aria-describedby=\"basic-addon1\"></div></td>";
    tbody+="<td></td>";
    tbody+="<td ><div class=\"btn-group w-100\"><button id=\"editbtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" onclick=\"editHarvesterItem("+i+")\" disabled>Edit</button>";    
    tbody+="<button id=\"updatebtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" onclick=\"updateHarvesterItem("+i+")\">Update</button>";
    tbody+="<button id=\"removebtn_"+i+"\" type=\"button\" class=\"btn btn-sm btn-outline-secondary\" onclick=\"removeHarvesterItem("+i+")\">";
    tbody+="Remove</button></div></td>";
    
    document.getElementById("h_"+i).innerHTML = tbody;
};

function updateHarvesterItem(i) {
  console.log('update:',i);
    
  var id = Hlist[i][0];
  var name = document.getElementById("name_"+i).value;
  var symbol = document.getElementById("symbol_"+i).value;
  var active = document.getElementById("active_"+i).checked?1:0;
  var record = [id,name,symbol,active];
  socket.emit('update_harvester',record);
    
  renderHlist = true; 
  socket.emit('get_harvester_list');
};

function removeHarvesterItem(i) {
  console.log('remove:',i);
    
  var id = Hlist[i][0];
  var name = document.getElementById("name_"+i).value;
  var symbol = document.getElementById("symbol_"+i).value;
  var active = document.getElementById("active_"+i).checked?1:0;
  var record = [id,name,symbol,active];
  socket.emit('remove_harvester',record);
    
  
  renderHlist = true;  
  socket.emit('get_harvester_list');  
};

function createHarvesterItem() {
  console.log('create');
    
  var name = document.getElementById("newname").value;
  var symbol = document.getElementById("newsymbol").value;
  var active = 0;
  var record = [name,symbol,active];
  socket.emit('create_harvester',record);
    
  renderHlist = true;   
  socket.emit('get_harvester_list');
 
};

function turnOffRender() {
    renderHlist = false;
}