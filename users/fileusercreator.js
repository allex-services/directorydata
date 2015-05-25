function createFileUser(execlib,ParentUser){

  if(!ParentUser){
    ParentUser = execlib.execSuite.ServicePack.Service.prototype.userFactory.get('user');
  }

  function FileUser(prophash){
    ParentUser.call(this,prophash);
  }
  ParentUser.inherit(FileUser,require('../methoddescriptors/fileuser'),[/*visible state fields here*/]/*or a ctor for StateStream filter*/,require('../visiblefields/fileuser'));
  FileUser.prototype.__cleanUp = function(){
    ParentUser.prototype.__cleanUp.call(this);
  };

  return FileUser;
}

module.exports = createFileUser;
