function createDirectoryDataUserTester(execlib,Tester){
  var lib = execlib.lib,
      q = lib.q;

  function DirectoryDataUserTester(prophash,client){
    Tester.call(this,prophash,client);
    console.log('runNext finish');
    lib.runNext(this.finish.bind(this,0));
  }
  lib.inherit(DirectoryDataUserTester,Tester);

  return DirectoryDataUserTester;
}

module.exports = createDirectoryDataUserTester;
