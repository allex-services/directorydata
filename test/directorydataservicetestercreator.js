function createDirectoryDataServiceTester(execlib,Tester){
  var lib = execlib.lib,
      q = lib.q;

  function DirectoryDataServiceTester(prophash,client){
    Tester.call(this,prophash,client);
    console.log('runNext finish');
    lib.runNext(this.finish.bind(this,0));
  }
  lib.inherit(DirectoryDataServiceTester,Tester);

  return DirectoryDataServiceTester;
}

module.exports = createDirectoryDataServiceTester;
