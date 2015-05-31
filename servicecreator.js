var fs = require('fs');

function createDirectoryDataService(execlib,ParentServicePack){
  var ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite,
    lib = execlib.lib;

  function factoryCreator(parentFactory){
    return {
      'service': require('./users/serviceusercreator')(execlib,parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib,parentFactory.get('user')),
      'fileuser': require('./users/fileusercreator')(execlib,parentFactory.get('user')) 
    };
  }

  function DirectoryDataService(prophash){
    ParentService.call(this,prophash);
    if(!('path' in prophash)){
      throw new lib.Error('No path in property hash');
    }
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
