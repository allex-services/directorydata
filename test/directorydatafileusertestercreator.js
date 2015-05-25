function createDirectoryDataFileuserTester(execlib,Tester){
  var lib = execlib.lib,
      q = lib.q;

  function DirectoryDataFileuserTester(prophash,client){
    Tester.call(this,prophash,client);
    console.log('runNext finish');
    lib.runNext(this.finish.bind(this,0));
  }
  lib.inherit(DirectoryDataFileuserTester,Tester);

  return DirectoryDataFileuserTester;
}

module.exports = createDirectoryDataFileuserTester;
