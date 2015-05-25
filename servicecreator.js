function createDirectoryDataService(execlib,ParentServicePack){
  var ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite;

  function factoryCreator(parentFactory){
    return {
      'service': require('./users/serviceusercreator')(execlib,parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib,parentFactory.get('user')),
      'fileuser': require('./users/fileusercreator')(execlib,parentFactory.get('user')) 
    };
  }

  function DirectoryDataService(prophash){
    ParentService.call(this,prophash);
  }
  ParentService.inherit(DirectoryDataService,factoryCreator,require('./storagedescriptor'));
  DirectoryDataService.prototype.__cleanUp = function(){
    ParentService.prototype.__cleanUp.call(this);
  };
  DirectoryDataService.prototype.preProcessUserHash = function(userhash){
    console.log('userhash',userhash);
    ParentService.prototype.preProcessUserHash.call(this,userhash);
  };
  DirectoryDataService.prototype.createStorage = function(storagedescriptor){
    return ParentService.prototype.createStorage.call(this,storagedescriptor);
  };
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
