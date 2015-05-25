var execlib=require('hers_exectesting')();

execlib.test({
  debug_brk: false,
  debug: false,
  name:'DirectoryData',
  modulepath:'./index.js',
  propertyhash: {
  }
},{
  debug_brk: false,
  debug: false,
  tests:[
  {
    count:2,
    role: 'service',
    tester:{
      count:2,
      modulepath:'./test/directorydataservicetestercreator',
      propertyhash:{
      }
    }
  },
  {
    count:2,
    role: 'user',
    tester:{
      count:2,
      modulepath:'./test/directorydatausertestercreator',
      propertyhash:{
      }
    }
  },
  {
    count:2,
    role: 'fileuser',
    tester:{
      count:2,
      modulepath:'./test/directorydatafileusertestercreator',
      propertyhash:{
      }
    }
  } 
  ]
});

