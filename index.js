"use strict";
/*global Buffer*/

var crypto = require('crypto');
var path = require('path');

var constants = require('oma-constants');
var imageDimensions = require('image-size');
var uglify = require('uglify-js');
var util = require('oma-util');

var Datauri = require('datauri');

var assetPath = {
  bootScript: constants.module.bootScript,
  bundleScriptsHome: constants.archive.bundleScripts.home + '/',
  configScript: constants.module.configScript,
  classHome: constants.module.classScripts.home + '/',
  configHome: constants.module.configScripts.home + '/',
  publicHome: constants.module.publicAssets.home + '/',
  bundleLoader: constants.bundle.loader + '.js',
  bundleMeta: constants.bundle.loader + '.json',
  bundleMini: constants.bundle.loader + '.min.js'
};

function datafy(extension, buffer) {
  var uri = new Datauri();
  uri.format(extension, buffer);
  return uri.content;
};

module.exports = function (archivePath, bundleDirectory) {
  return openArchive(archivePath)
    .then(function (mainArchive) {
      // get bundle configuration scripts from main archive
      var scripts = util.selectEntries(mainArchive.entries, assetPath.bundleScriptsHome, '.js');
      return Promise.all(Object.keys(scripts).map(function (bundleName) {
        return util.unzipText(mainArchive.file, scripts[bundleName])
          .then(function (source) {
            // execute scripts to obtain bundle configuration
            var config = {};
            Function('return ' + source)()(config);
            return config;
          })
          .then(function (bundleConfig) {
            return publishBundle(mainArchive, bundleName, bundleConfig, bundleDirectory);
          })
          ;
      }));
    });
};

// open versioned archive at given path 
function openArchive(archivePath) {
  var archiveDirectory = path.dirname(archivePath);
  var archiveName = path.basename(archiveDirectory);
  var archiveVersion = path.basename(archivePath, '.zip');
  var patternArchiveName = constants.archive.pattern.name;
  var patternArchiveVersion = constants.archive.pattern.version;
  if (!archiveName.match(patternArchiveName) || !archiveVersion.match(patternArchiveVersion)) {
    throw new Error('Invalid archive: ' + archivePath);
  }
  return util.unzip(archivePath)
    .then(function (archive) {
      archive.home = path.dirname(archiveDirectory);
      archive.name = archiveName;
      archive.version = archiveVersion;
      var modules = archive.modules = {};
      // collect assets of modules
      for (var entry in archive.entries) {
        var moduleName = entry.substring(0, entry.indexOf('/'));
        if (moduleName.indexOf('.') > 0) {
          var archivedModule = modules[moduleName] || (modules[moduleName] = { assets: {} });
          // link module specification to originating archive
          archivedModule.archive = archive;
          archivedModule.assets[entry.substring(moduleName.length + 1)] = archive.entries[entry];
        }
      }
      // object with name, home, version and modules property
      return archive;
    })
    ;
}

// publish bundle whose configuration is part of main archive
function publishBundle(mainArchive, bundleName, bundleConfig, bundleDirectory) {
  // collect all modules from source archives
  return bundleModules(mainArchive, bundleName, bundleConfig)
    .then(function (modules) {
      // determine directory where bundle should be released
      var releaseId = releaseBundle(mainArchive, bundleConfig, modules);
      var releaseHome = bundleDirectory + '/' + bundleName + '/' + releaseId;
      return util.stat(releaseHome)
        .then(null, function () {
          // process and publish modules if release does not yet exist
          return Promise.all(Object.keys(modules).map(function (moduleName) {
            return processModule(releaseHome, modules[moduleName]);
          }))
            .then(function () {
              var bundled = { name: bundleName, config: bundleConfig, modules: modules };
              return publishModules(mainArchive, releaseHome, bundled);
            })
            ;
        })
        .then(function () {
          return releaseHome;
        })
        ;
    })
    ;
}

// collect bundled modules from source archives
function bundleModules(mainArchive, bundleName, bundleConfig) {
  var archives = {}, bundledModules = {};
  archives[mainArchive.name] = mainArchive;
  var externals = bundleConfig.versions || {};
  delete externals[mainArchive.name];
  // find and open external archives (in same directory as main archive)
  return Promise.all(Object.keys(externals).map(function (externalName) {
    var externalVersion = externals[externalName];
    return findBestArchive(mainArchive.home, externalName, externalVersion)
      .then(function (externalArchive) {
        if (!externalArchive) {
          var missing = 'Missing archive ' + externalName + ' ' + externalVersion;
          throw new Error(assetPath.bundleScriptsHome + bundleName + ': ' + missing);
        }
        archives[externalName] = externalArchive;
      })
      ;
  }))
    .then(function () {
      // collect bundled modules and report conflicts
      var includes = bundleConfig.includes || [''], excludes = bundleConfig.excludes || [];
      for (var archiveName in archives) {
        var modules = archives[archiveName].modules;
        for (var moduleName in modules) {
          var startsWith = util.startsWith.bind(null, moduleName);
          if (includes.some(startsWith) && !excludes.some(startsWith)) {
            if (bundledModules[moduleName]) {
              var otherName = bundledModules[moduleName].archive.name;
              throw new Error(moduleName + ' in archives ' + otherName + ' and ' + archiveName);
            }
            bundledModules[moduleName] = modules[moduleName];
          }
        }
      }
      return bundledModules;
    })
    ;
}

// open archive with highest version that satifies dependency on external archive
function findBestArchive(homeDirectory, archiveName, archiveVersion) {
  var archivePath = homeDirectory + '/' + archiveName + '/' + constants.archive.version + '.zip';
  var versions = {};
  return util.mapFiles(archivePath, function (file, cb) {
    versions[path.basename(file.path, '.zip')] = file.path;
    cb(null);
  })
    .then(function () {
      var bestVersion = util.bestVersion(Object.keys(versions), archiveVersion);
      if (bestVersion) {
        return openArchive(versions[bestVersion]);
      }
    })
    ;
}

// compute directory name for bundle release
function releaseBundle(mainArchive, bundleConfig, modules) {
  // collect archives from where bundle configuration and bundled modules originate
  var moduleOrigins = ['=' + mainArchive.name + '/' + mainArchive.version];
  Object.keys(modules).sort().forEach(function (moduleName, index) {
    var bundledModule = modules[moduleName];
    bundledModule.configs = [];
    bundledModule.classes = {};
    bundledModule.index = index + 1;
    if (bundledModule.assets[assetPath.bootScript]) {
      if (bundleConfig.boot) {
        throw new Error('Boot conflict between ' + bundleConfig.boot + ' and ' + moduleName);
      }
      bundleConfig.boot = moduleName;
    }
    var moduleArchive = bundledModule.archive;
    moduleOrigins.push(moduleName + '=' + moduleArchive.name + '/' + moduleArchive.version);
  });
  // calculate release id from md5 signature of module origins
  var release = bundleConfig.release = moduleOrigins.join();
  return crypto.createHash('md5').update(release, 'utf8').digest('base64')
    .replace(/=*$/, '').replace(/\//g, '-').replace(/\+/g, '_');
  ;
}

// process assets of module
function processModule(releaseHome, bundledModule) {
  var bundledAssets = bundledModule.assets;
  var configAssets = util.selectEntries(bundledAssets, assetPath.configHome, '.js');
  var classAssets = util.selectEntries(bundledAssets, assetPath.classHome, '.js');
  var publicAssets = util.selectEntries(bundledAssets, assetPath.publicHome);
  var processingAssets = [
    // collect primary configuration script 
    util.unzipText(bundledModule.archive.file, bundledAssets[assetPath.configScript])
      .then(function (configSource) {
        bundledModule.configs.unshift(configSource);
      })
  ];
  // promise to process selected assets
  function processAssets(assets, processor) {
    Array.prototype.push.apply(processingAssets, Object.keys(assets).map(processor));
  }
  // collect secondary configuration script from subdirectory
  processAssets(configAssets, function (configPath) {
    return util.unzipText(bundledModule.archive.file, configAssets[configPath])
      .then(function (configSource) {
        bundledModule.configs.push(configSource);
      })
      ;
  });
  // collect class scripts
  processAssets(classAssets, function (classPath) {
    return util.unzipText(bundledModule.archive.file, classAssets[classPath])
      .then(function (classSource) {
        bundledModule.classes[classPath.replace(util.vseps, '.')] = classSource;
      })
      ;
  });
  // copy public assets
  processAssets(publicAssets, function (publicPath) {
    var input = util.unzipStream(bundledModule.archive.file, publicAssets[publicPath]);
    var output = util.openWriteStream(releaseHome + '/' + bundledModule.index + '/' + publicPath);
    return util.copy(input, output);
  });
  // minify JavaScript assets
  processAssets(publicAssets, function (publicPath) {
    if (util.endsWith(publicPath, '.js') && !util.endsWith(publicPath, '.min.js')) {
      var javaScriptAsset = publicAssets[publicPath];
      return util.unzipText(bundledModule.archive.file, javaScriptAsset)
        .then(function (scriptSource) {
          var miniSource = uglify.minify(scriptSource, { fromString: true }).code;
          javaScriptAsset.minifiedSize = Buffer.byteLength(miniSource);
          var miniPath = publicPath.replace(/js$/, 'min.js');
          var outputPath = releaseHome + '/' + bundledModule.index + '/' + miniPath;
          return util.copy(util.streamInput(miniSource), util.openWriteStream(outputPath));
        })
        ;
    }
  });
  // datafy small binary assets
  processAssets(publicAssets, function (publicPath) {
    var extension = path.extname(publicPath).substring(1);
    var publicAsset = publicAssets[publicPath];
    if (datafyExtensions[extension] && publicAsset.uncompressedSize <= datafyLimit) {
      return util.unzipBuffer(bundledModule.archive.file, publicAsset)
        .then(function (data) {
          publicAsset.datafied = datafy(extension, data);
        })
        ;
    }
  });
  // improve info about large graphics assets
  processAssets(publicAssets, function (publicPath) {
    var extension = path.extname(publicPath).substring(1);
    var publicAsset = publicAssets[publicPath];
    if (graphicsExtensions[extension] && publicAsset.uncompressedSize > datafyLimit) {
      return util.unzipBuffer(bundledModule.archive.file, publicAsset)
        .then(function (data) {
          var imageDims = imageDimensions(data);
          publicAsset.imageHeight = imageDims.height;
          publicAsset.imageWidth = imageDims.width;
        })
        ;
    }
  });
  return Promise.all(processingAssets);
}
var datafyLimit = constants.tool.datafy.limit, datafyExtensions = {}, graphicsExtensions = {};
util.fileExtensions(constants.category, constants.tool.datafy.category)
  .forEach(function (extension) {
    datafyExtensions[extension] = true;
  })
;
util.fileExtensions(constants.category, 'gfx')
  .forEach(function (extension) {
    graphicsExtensions[extension] = true;
  })
;

// publish new release of bundled modules
function publishModules(mainArchive, releaseHome, bundled) {
  return Promise.all([
    createBundlePrologue(bundled.name, bundled.modules, bundled.config.boot),
    createBundleSpecs(mainArchive, bundled.name, bundled.modules, bundled.config.release)
  ])
    .then(function (sources) {
      var loaderPath = releaseHome + '/' + assetPath.bundleLoader;
      var miniPath = releaseHome + '/' + assetPath.bundleMini;
      var metaPath = releaseHome + '/' + assetPath.bundleMeta;
      var loaderSource = sources[0] + '.bundle(' + sources[1] + ');'
      var miniSource = uglify.minify(loaderSource, { fromString: true }).code;
      var moduleSpecs = evaluateModuleSpecs(sources[1]);
      var metaSource = JSON.stringify(createBundleMeta(moduleSpecs));
      var outputOptions = { defaultEncoding: 'utf8' };
      var loaderOutput = util.openWriteStream(loaderPath, outputOptions);
      var miniOutput = util.openWriteStream(miniPath, outputOptions);
      var metaOutput = util.openWriteStream(metaPath, outputOptions);
      return Promise.all([
        util.copy(util.streamInput(loaderSource), loaderOutput),
        util.copy(util.streamInput(miniSource), miniOutput),
        util.copy(util.streamInput(metaSource), metaOutput)
      ]);
    })
    ;
}

// create prologue with appropriate loader for bundled modules
function createBundlePrologue(bundleName, bundledModules, bootName) {
  if (bootName) {
    // use boot script to load modules
    var bootModule = bundledModules[bootName];
    var bootScript = bootModule.assets[assetPath.bootScript];
    return util.unzipText(bootModule.archive.file, bootScript)
      .then(function (bootSource) {
        return '(' + bootSource + '(\'' + bundleName + '\',\'' + bootName + '\'))';
      })
      ;
  } else {
    // rely on string method to load modules
    return '\'' + bundleName + '\'';
  }
}

// create bundle and module specifications
function createBundleSpecs(mainArchive, bundleName, modules, release) {
  var generated = [], generate = generated.push.bind(generated);
  generate('{\'\':{\'\':[');
  return generateBundleConfigs(generate, mainArchive, bundleName, release)
    .then(function () {
      generate(']}');
      var chainedPromise = Promise.resolve();
      Object.keys(modules).sort().forEach(function (moduleName) {
        chainedPromise = chainedPromise
          .then(function () {
            generate(',\'', moduleName, '\':');
            return generateModuleSpec(generate, modules[moduleName]);
          })
        ;
      });
      return chainedPromise;
    })
    .then(function () {
      generate('}');
      return generated.join('');
    })
    ;
}

// generate configuration scripts of bundle loader
function generateBundleConfigs(generate, mainArchive, bundleName, release) {
  generate('function(bundle){',
    '"use strict";',
    'bundle.releases={',
  // include configuration info about modules in this release
    '\'', release.replace(/=/g, '\':\'').replace(/,/g, '\',\''), '\'',
    '};'
    );
  // include configuration info about source archives in this release
  var sourceVersions = computeSourceVersions(release);
  generate('bundle.sources={');
  Object.keys(sourceVersions).sort().forEach(function (archiveName, i) {
    generate(i ? ',\'' : '\'', archiveName, '\':\'', sourceVersions[archiveName], '\'');
  });
  generate('};');
  generate(
    'bundle.publishes={',
    '\'', assetPath.bundleLoader, '\':-1,',
    '\'', assetPath.bundleMini, '\':-1,',
    '\'', assetPath.bundleMeta, '\':-1',
    '};',
    '},'
    );
  var configPath = assetPath.bundleScriptsHome + bundleName + '.js';
  return util.unzipText(mainArchive.file, mainArchive.entries[configPath])
    .then(function (configSource) {
      // bundle loaders are public assets
      generate(configSource);
    })
    ;
}

// compute object that maps archive names to versions from bundle release
function computeSourceVersions(release) {
  var archiveVersions = {};
  release.split(',').map(function (equation) {
    var archiveVersion = equation.substring(equation.indexOf('=') + 1).split('/');
    archiveVersions[archiveVersion[0]] = archiveVersion[1];
  });
  return archiveVersions;
}

// generate module specification of bundle loader
function generateModuleSpec(generate, bundledModule) {
  var archive = bundledModule.archive, assets = bundledModule.assets;
  return util.unzipText(archive.file, assets[assetPath.configScript])
    .then(function (scriptSource) {
      generate('{\'\':[', scriptSource);
    })
    .then(function () {
      var chainedPromise = Promise.resolve();
      var secondaryScripts = util.selectEntries(assets, assetPath.configHome);
      Object.keys(secondaryScripts).forEach(function (configName) {
        chainedPromise = chainedPromise
          .then(function () {
            generate(',');
            return util.unzipText(archive.file, secondaryScripts[configName]).then(generate);
          });
      })
      return chainedPromise;
    })
    .then(function () {
      var publicAssets = util.selectEntries(assets, assetPath.publicHome);
      if (util.hasEnumerables(publicAssets)) {
        generatePublicSpecs(generate, archive, publicAssets);
      }
    })
    .then(function () {
      generate(']');
      var classes = bundledModule.classes;
      Object.keys(classes).sort().forEach(function (className) {
        generate(',\'', className, '\':', classes[className]);
      });
      generate('}');
    })
    ;
}

// generate info about public assets
function generatePublicSpecs(generate, archive, assets) {
  generate(',function(module){"use strict";module.publishes={');
  Object.keys(assets).forEach(function (publicPath, ix) {
    var publicAsset = assets[publicPath], size = publicAsset.uncompressedSize;
    generate(ix ? ',' : '', '\'', publicPath, '\':');
    if (publicAsset.datafied) {
      generate('{bytes:', size, ',data64:\'', publicAsset.datafied, '\'}');
    } else if (publicAsset.imageHeight) {
      var height = publicAsset.imageHeight, width = publicAsset.imageWidth;
      generate('{bytes:', size, ',height:', height, ',width:', width, '}');
    } else {
      generate(size);
    }
    if (publicAsset.minifiedSize) {
      generate(',\'', publicPath.replace(/js$/, 'min.js'), '\':', publicAsset.minifiedSize);
    }
  });
  generate('};}');
}

// generate meta object that describes the modules in a bundle
function createBundleMeta(moduleSpecs) {
  // extract release info from bundle config that maps bundled modules to archives
  var moduleArchives = collectModuleConfig(moduleSpecs['']['']).releases;
  var sortedNames = Object.keys(moduleArchives).sort();
  var metaObject = {};
  // collect more meta info about modules
  for (var moduleName in moduleArchives) {
    var moduleSpec = moduleSpecs[moduleName];
    var moduleConfig = collectModuleConfig(moduleSpec['']);
    var dependencies = moduleConfig.depends || [];
    // collect dependencies from class scripts
    for (var className in moduleSpec) {
      if (className && Array.isArray(moduleSpec[className])) {
        moduleSpec[className].forEach(function (dependencyName) {
          if (dependencies.indexOf(dependencyName) < 0) {
            dependencies.push(dependencyName);
          }
        });
      }
    }
    var archiveVersion = moduleArchives[moduleName].split('/');
    var datatypes = moduleConfig.datatypes;
    metaObject[moduleName] = {
      description: moduleConfig.description || 'Undocumented',
      archive: { name: archiveVersion[0], version: archiveVersion[1] },
      depends: dependencies.length ? dependencies.sort() : undefined,
      index: sortedNames.indexOf(moduleName),
      optional: typeof moduleConfig.test === 'function' ? 'y' : undefined,
      datatypes: util.hasEnumerables(datatypes) ? { _: flatTypespace('', datatypes) } : undefined
    };
  }
  return { _: metaObject };
}

function evaluateModuleSpecs(source) {
  // install temporary string method
  String.prototype.subclass = function () {
    for (var i = 0, n = arguments.length - 1; i < n; ++i) {
      if (Array.isArray(arguments[i])) {
        return arguments[i];
      }
    }
  };
  // evaluate module specifications
  var specs = Function('return ' + source)();
  delete String.prototype.subclass;
  return specs;
}

// sequence of configure closures computes configuration
function collectModuleConfig(configureClosures) {
  var config = {};
  configureClosures.forEach(function (closure) { closure(config); });
  return config;
}

// convert configured datatypes to flat typespace
var TypenameKey = /^[A-Z][0-9A-Za-z]+$/;
function flatTypespace(home, definitions, flat) {
  flat = flat || {};
  for (var key in definitions) {
    var source = definitions[key];
    var name = home ? home + '.' + key : key;
    if (typeof source === 'string') {
      flat[name] = source;
    } else {
      for (var probe in source) {
        if (TypenameKey.test(probe)) {
          flatTypespace(name, source, flat);
        } else {
          flat[name] = flatRecordType(source);
        }
        break;
      }
    }
  }
  return flat;
}

// compute configured record type
function flatRecordType(fields) {
  var accu = [];
  if (fields.$macro) {
    accu.push('(');
    for (var i = 0, n = fields.$macro.length; i < n; ++i) {
      accu.push(i ? ',' : '', fields.$macro[i]);
    }
    accu.push(')');
  }
  if (fields.$super) {
    accu.push(fields.$super, '+');
  }
  accu.push('{');
  var comma = '';
  for (var key in fields) {
    if (key.charAt(0) !== '$') {
      var source = fields[key];
      accu.push(comma, key, ':', typeof source === 'string' ? source : flatRecordType(source));
      comma = ',';
    }
  }
  accu.push('}');
  return accu.join('');
}
