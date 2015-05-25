function createFileuserSink(execlib,ParentSink){

  if(!ParentSink){
    ParentSink = execlib.execSuite.registry.get('.').SinkMap.get('user');
  }

  function FileuserSink(prophash,client){
    ParentSink.call(this,prophash,client);
  }
  ParentSink.inherit(FileuserSink,require('../methoddescriptors/fileuser'),require('../visiblefields/fileuser'),require('../storagedescriptor'));
  FileuserSink.prototype.__cleanUp = function(){
    ParentSink.prototype.__cleanUp.call(this);
  };
  return FileuserSink;
}

module.exports = createFileuserSink;
