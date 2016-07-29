"use strict";
/*global Buffer*/

const crypto = require('crypto');
const path = require('path');

const constants = require('oma-constants');
const imageDimensions = require('image-size');
const uglify = require('uglify-js');
const util = require('oma-util');

const Datauri = require('datauri');

const assetPath = {
  bootScript: constants.module.bootScript,
  bundleScriptsHome: `${constants.archive.bundleScripts.home}/`,
  configScript: constants.module.configScript,
  classHome: `${constants.module.classScripts.home}/`,
  configHome: `${constants.module.configScripts.home}/`,
  publicHome: `${constants.module.publicAssets.home}/`,
  bundleLoader: `${constants.bundle.file}.js`,
  bundleMeta: `${constants.bundle.file}.json`,
  bundleMini: `${constants.bundle.file}.min.js`
};

const datafyLimit = constants.tool.datafy.limit, datafyExtensions = {}, graphicsExtensions = {};
for (const extension of util.fileExtensions(constants.category, constants.tool.datafy.category)) {
  datafyExtensions[extension] = true;
}
for (const extension of util.fileExtensions(constants.category, 'gfx')) {
  graphicsExtensions[extension] = true;
}

function datafy(extension, buffer) {
  const uri = new Datauri();
  uri.format(extension, buffer);
  return uri.content;
};

module.exports = (archivePath, bundleDirectory) => openArchive(archivePath)
  .then(mainArchive => {
    // get bundle configuration scripts from main archive
    const scripts = util.selectEntries(mainArchive.entries, assetPath.bundleScriptsHome, '.js');
    return Promise.all(Object.keys(scripts).map(bundleName =>
      util.unzipText(mainArchive.file, scripts[bundleName])
        .then(source => {
          // execute scripts to obtain bundle configuration
          const bundleConfig = {};
          Function(`return ${source}`)()(bundleConfig);
          return bundleConfig;
        })
        .then(config => publishBundle(mainArchive, bundleName, config, bundleDirectory))
    ));
  });


// open versioned archive at given path 
function openArchive(archivePath) {
  const archiveVersion = path.basename(path.dirname(archivePath));
  const archiveName = path.basename(path.dirname(path.dirname(archivePath)));
  const patternArchiveName = constants.archive.pattern.name;
  const patternArchiveVersion = constants.archive.pattern.version;
  if (!archiveName.match(patternArchiveName) || !archiveVersion.match(patternArchiveVersion)) {
    throw new Error(`Invalid archive: ${archivePath}`);
  }
  return util.unzip(archivePath)
    .then(archive => {
      archive.path = archivePath;
      archive.name = archiveName;
      archive.version = archiveVersion;
      const modules = archive.modules = {};
      // collect assets of modules
      for (const entry in archive.entries) {
        const moduleName = entry.substring(0, entry.indexOf('/'));
        if (moduleName.indexOf('.') > 0) {
          const archivedModule = modules[moduleName] || (modules[moduleName] = { assets: {} });
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
    .then(modules => {
      // determine directory where bundle should be released
      const releaseId = releaseBundle(mainArchive, bundleConfig, modules);
      const releaseHome = `${bundleDirectory}/${bundleName}/${releaseId}`;
      return util.stat(releaseHome)
        .then(null, () => {
          // process and publish modules if release does not yet exist
          const processing = moduleName => processModule(releaseHome, modules[moduleName]);
          return Promise.all(Object.keys(modules).map(processing))
            .then(() => {
              const bundled = { name: bundleName, config: bundleConfig, modules: modules };
              return publishModules(mainArchive, releaseHome, bundled);
            })
            ;
        })
        .then(() => releaseHome)
        ;
    })
    ;
}

// collect bundled modules from source archives
function bundleModules(mainArchive, bundleName, bundleConfig) {
  const archives = {}, bundledModules = {};
  archives[mainArchive.name] = mainArchive;
  const externals = bundleConfig.versions || {};
  delete externals[mainArchive.name];
  // find and open external archives (relative to same directory as main archive)
  return Promise.all(Object.keys(externals).map(externalName => {
    const externalVersion = externals[externalName];
    const archiveHome = path.dirname(path.dirname(path.dirname(mainArchive.path)));
    return findBestArchive(archiveHome, externalName, externalVersion)
      .then(externalArchive => {
        if (!externalArchive) {
          const missing = `Missing archive ${externalName} ${externalVersion}`;
          throw new Error(`${assetPath.bundleScriptsHome}${bundleName}: ${missing}`);
        }
        archives[externalName] = externalArchive;
      })
      ;
  }))
    .then(function() {
      // collect bundled modules and report conflicts
      const includes = bundleConfig.includes || [''], excludes = bundleConfig.excludes || [];
      for (const archiveName in archives) {
        const modules = archives[archiveName].modules;
        for (const moduleName in modules) {
          const patternMatch = pattern => util.startsWith(moduleName, pattern);
          if (includes.some(patternMatch) && !excludes.some(patternMatch)) {
            if (bundledModules[moduleName]) {
              const otherName = bundledModules[moduleName].archive.name;
              throw new Error(`${moduleName} in archives ${otherName} and ${archiveName}`);
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
function findBestArchive(homeDir, archiveName, archiveVersion) {
  const versionPattern = constants.archive.version;
  const archivePath = `${homeDir}/${archiveName}/${versionPattern}/${constants.archive.file}.zip`;
  const versions = {};
  return util.eachFile(archivePath, file => {
    versions[path.basename(path.dirname(file.path))] = file.path;
  })
    .then(() => {
      const bestVersion = util.bestVersion(Object.keys(versions), archiveVersion);
      if (bestVersion) {
        return openArchive(versions[bestVersion]);
      }
    })
    ;
}

// compute directory name for bundle release
function releaseBundle(mainArchive, bundleConfig, modules) {
  // collect archives from where bundle configuration and bundled modules originate
  const moduleOrigins = [`=${mainArchive.name}/${mainArchive.version}`];
  Object.keys(modules).sort().forEach((moduleName, index) => {
    const bundledModule = modules[moduleName];
    bundledModule.configs = [];
    bundledModule.classes = {};
    bundledModule.ordinal = index + 1;
    if (bundledModule.assets[assetPath.bootScript]) {
      if (bundleConfig.boot) {
        throw new Error(`Boot conflict between ${bundleConfig.boot} and ${moduleName}`);
      }
      bundleConfig.boot = moduleName;
    }
    const moduleArchive = bundledModule.archive;
    moduleOrigins.push(`${moduleName}=${moduleArchive.name}/${moduleArchive.version}`);
  });
  // calculate release id from md5 signature of module origins
  const release = bundleConfig.release = moduleOrigins.join();
  return crypto.createHash('md5').update(release, 'utf8').digest('base64')
    .replace(/=*$/, '').replace(/\//g, '-').replace(/\+/g, '_');
  ;
}

// process assets of module
function processModule(releaseHome, bundledModule) {
  const bundledAssets = bundledModule.assets;
  const configAssets = util.selectEntries(bundledAssets, assetPath.configHome, '.js');
  const classAssets = util.selectEntries(bundledAssets, assetPath.classHome, '.js');
  const publicAssets = util.selectEntries(bundledAssets, assetPath.publicHome);
  const processingAssets = [
    // collect primary configuration script 
    util.unzipText(bundledModule.archive.file, bundledAssets[assetPath.configScript])
      .then(configSource => { bundledModule.configs.unshift(configSource); })
  ];
  // promise to process selected assets
  function processAssets(assets, processor) {
    processingAssets.push(...Object.keys(assets).map(processor));
  }
  // collect secondary configuration script from subdirectory
  processAssets(configAssets, configPath =>
    util.unzipText(bundledModule.archive.file, configAssets[configPath])
      .then(configSource => { bundledModule.configs.push(configSource); })
  );
  // collect class scripts
  processAssets(classAssets, classPath =>
    util.unzipText(bundledModule.archive.file, classAssets[classPath])
      .then(classSource => {
        bundledModule.classes[classPath.replace(util.vseps, '.')] = classSource;
      })
  );
  // copy public assets
  processAssets(publicAssets, publicPath => {
    const input = util.unzipStream(bundledModule.archive.file, publicAssets[publicPath]);
    const outputPath = `${releaseHome}/${bundledModule.ordinal}/${publicPath}`;
    const output = util.openWriteStream(outputPath);
    return util.copy(input, output);
  });
  // minify JavaScript assets
  /*
  processAssets(publicAssets, publicPath => {
    if (util.endsWith(publicPath, '.js') && !util.endsWith(publicPath, '.min.js')) {
      const javaScriptAsset = publicAssets[publicPath];
      return util.unzipText(bundledModule.archive.file, javaScriptAsset)
        .then(scriptSource => {
          const miniSource = uglify.minify(scriptSource, { fromString: true }).code;
          javaScriptAsset.minifiedSize = Buffer.byteLength(miniSource);
          const miniPath = publicPath.replace(/js$/, 'min.js');
          const outputPath = `${releaseHome}/${bundledModule.ordinal}/${miniPath}`;
          return util.copy(util.streamInput(miniSource), util.openWriteStream(outputPath));
        })
        ;
    }
  });
  */
  // datafy small binary assets
  processAssets(publicAssets, publicPath => {
    const extension = path.extname(publicPath).substring(1);
    const publicAsset = publicAssets[publicPath];
    if (datafyExtensions[extension] && publicAsset.uncompressedSize <= datafyLimit) {
      return util.unzipBuffer(bundledModule.archive.file, publicAsset)
        .then(data => { publicAsset.datafied = datafy(extension, data); })
        ;
    }
  });
  // improve info about large graphics assets
  processAssets(publicAssets, publicPath => {
    const extension = path.extname(publicPath).substring(1);
    const publicAsset = publicAssets[publicPath];
    if (graphicsExtensions[extension] && publicAsset.uncompressedSize > datafyLimit) {
      return util.unzipBuffer(bundledModule.archive.file, publicAsset)
        .then(data => {
          const dimensions = imageDimensions(data);
          publicAsset.imageHeight = dimensions.height;
          publicAsset.imageWidth = dimensions.width;
        })
        ;
    }
  });
  // promise to process all assets
  return Promise.all(processingAssets);
}

// publish new release of bundled modules
function publishModules(mainArchive, releaseHome, bundled) {
  // directory 0 holds assets of anonymous module
  const loaderPath = `${releaseHome}/0/${assetPath.bundleLoader}`;
  const miniPath = `${releaseHome}/0/${assetPath.bundleMini}`;
  const metaPath = `${releaseHome}/0/${assetPath.bundleMeta}`;
  return Promise.all([
    createBundlePrologue(bundled.name, bundled.modules, bundled.config.boot),
    createBundleSpecs(mainArchive, bundled.name, bundled.modules, bundled.config.release)
  ])
    .then(sources => {
      const loaderSource = `${sources[0]}.bundle(${sources[1]});`;
      // const miniSource = uglify.minify(loaderSource, { fromString: true }).code;
      const moduleSpecs = evaluateModuleSpecs(sources[1]);
      const outputOptions = { defaultEncoding: 'utf8' };
      const loaderOutput = util.openWriteStream(loaderPath, outputOptions);
      // const miniOutput = util.openWriteStream(miniPath, outputOptions);
      const metaOutput = util.openWriteStream(metaPath, outputOptions);
      return Promise.all([
        util.copy(util.streamInput(loaderSource), loaderOutput),
        // util.copy(util.streamInput(miniSource), miniOutput),
        util.copyJSON(createBundleMeta(moduleSpecs), metaOutput)
      ]);
    })
    ;
}

// create prologue with appropriate loader for bundled modules
function createBundlePrologue(bundleName, bundledModules, bootName) {
  if (bootName) {
    // use boot script to load modules
    const bootModule = bundledModules[bootName];
    const bootScript = bootModule.assets[assetPath.bootScript];
    return util.unzipText(bootModule.archive.file, bootScript)
      .then(bootSource => `(${bootSource}('${bundleName}','${bootName}'))`)
      ;
  } else {
    // rely on string method to load modules
    return `'${bundleName}'`;
  }
}

// create bundle and module specifications
function createBundleSpecs(mainArchive, bundleName, modules, release) {
  const generated = [], generate = generated.push.bind(generated);
  generate(`{'':{'':[`)
  return generateBundleConfigs(generate, mainArchive, bundleName, release)
    .then(() => {
      generate(']}');
      let chainedPromise = Promise.resolve();
      for (const moduleName of Object.keys(modules).sort()) {
        chainedPromise = chainedPromise
          .then(() => {
            generate(`,'${moduleName}':`)
            return generateModuleSpec(generate, modules[moduleName]);
          })
          ;
      }
      return chainedPromise;
    })
    .then(() => {
      generate('}');
      return generated.join('');
    })
    ;
}

// generate configuration scripts of bundle loader
function generateBundleConfigs(generate, mainArchive, bundleName, release) {
  // include configuration info about module origins in this release
  const origins = release.replace(/=/g, `':'`).replace(/,/g, `','`).replace(/\/[0-9.]+/g, '');
  generate('function(bundle){"use strict";');
  generate(`bundle.modules={'`, origins, `'};`);
  // include configuration info about source archives in this release
  const sourceVersions = computeSourceVersions(release);
  generate('bundle.archives={');
  Object.keys(sourceVersions).sort().forEach((archiveName, i) => {
    generate(i ? `,'` : `'`, archiveName, `':'`, sourceVersions[archiveName], `'`);
  });
  generate('};');
  generate('bundle.publishes={');
  generate(`'`, assetPath.bundleLoader, `':-1,`);
  generate(`'`, assetPath.bundleMini, `':-1,`);
  generate(`'`, assetPath.bundleMeta, `':-1`);
  generate('};');
  generate('},');
  const configPath = `${assetPath.bundleScriptsHome}${bundleName}.js`;
  return util.unzipText(mainArchive.file, mainArchive.entries[configPath])
    .then(configSource => { generate(configSource); })
    ;
}

// compute object that maps archive names to versions from bundle release
function computeSourceVersions(release) {
  const archiveVersions = {};
  release.split(',').map(equation => {
    const archiveVersion = equation.substring(equation.indexOf('=') + 1).split('/');
    archiveVersions[archiveVersion[0]] = archiveVersion[1];
  });
  return archiveVersions;
}

// generate module specification of bundle loader
function generateModuleSpec(generate, bundledModule) {
  const archive = bundledModule.archive, assets = bundledModule.assets;
  return util.unzipText(archive.file, assets[assetPath.configScript])
    .then(scriptSource => { generate(`{'':[`, scriptSource); })
    .then(() => {
      let chainedPromise = Promise.resolve();
      const secondaryScripts = util.selectEntries(assets, assetPath.configHome);
      for (const configName of Object.keys(secondaryScripts)) {
        chainedPromise = chainedPromise
          .then(() => {
            generate(',');
            return util.unzipText(archive.file, secondaryScripts[configName]).then(generate);
          })
          ;
      }
      return chainedPromise;
    })
    .then(() => {
      const publicAssets = util.selectEntries(assets, assetPath.publicHome);
      if (util.hasEnumerables(publicAssets)) {
        generatePublicSpecs(generate, archive, publicAssets);
      }
    })
    .then(() => {
      generate(']');
      const classes = bundledModule.classes;
      for (const className of Object.keys(classes).sort()) {
        generate(`,'`, className, `':`, classes[className]);
      }
      generate('}');
    })
    ;
}

// generate info about public assets
function generatePublicSpecs(generate, archive, assets) {
  generate(`,function(module){"use strict";`);
  generate('module.publishes={');
  Object.keys(assets).forEach(function(publicPath, i) {
    const publicAsset = assets[publicPath], size = publicAsset.uncompressedSize;
    generate(i ? ',' : '', `'`, publicPath, `':`);
    if (publicAsset.datafied) {
      generate('{size:', size, `,data64:'`, publicAsset.datafied, `'}`);
    } else if (publicAsset.imageHeight) {
      const height = publicAsset.imageHeight, width = publicAsset.imageWidth;
      generate('{size:', size, ',pixel:{height:', height, ',width:', width, '}}');
    } else {
      generate(size);
    }
    if (publicAsset.minifiedSize) {
      generate(`,'`, publicPath.replace(/js$/, 'min.js'), `':`, publicAsset.minifiedSize);
    }
  });
  generate('};');
  generate('}');
}

// generate meta object that describes the modules in a bundle
function createBundleMeta(moduleSpecs) {
  // extract release info from bundle config that maps bundled modules to archives
  const bundleConfig = collectModuleConfig(moduleSpecs['']['']);
  const moduleArchives = bundleConfig.modules, archiveVersions = bundleConfig.archives;
  const sortedNames = Object.keys(moduleArchives).sort();
  const metaObject = {};
  // collect more meta info about modules
  for (const moduleName in moduleArchives) {
    const moduleSpec = moduleSpecs[moduleName];
    const moduleConfig = collectModuleConfig(moduleSpec['']);
    const dependencies = moduleConfig.depends || [];
    const serviceProviders = moduleConfig.provides ? Object.keys(moduleConfig.provides) : [];
    // collect dependencies from class scripts
    for (const className in moduleSpec) {
      if (className && Array.isArray(moduleSpec[className])) {
        for (const dependencyName of moduleSpec[className]) {
          if (dependencies.indexOf(dependencyName) < 0) {
            dependencies.push(dependencyName);
          }
        }
      }
    }
    const archiveName = moduleArchives[moduleName], datatypes = moduleConfig.datatypes;
    metaObject[moduleName] = {
      description: moduleConfig.description || 'Undocumented',
      archive: { name: archiveName, version: archiveVersions[archiveName] },
      depends: dependencies.length ? dependencies.sort() : undefined,
      provides: serviceProviders.length ? serviceProviders.sort() : undefined,
      ordinal: sortedNames.indexOf(moduleName),
      optional: typeof moduleConfig.test === 'function' ? 'y' : undefined,
      datatypes: util.hasEnumerables(datatypes) ? { _: flatTypespace(datatypes) } : undefined
    };
  }
  return { _: metaObject };
}

function evaluateModuleSpecs(source) {
  // install temporary string method
  String.prototype.subclass = function() {
    const n = arguments.length - 1;
    for (let i = 0; i < n; ++i) {
      if (Array.isArray(arguments[i])) {
        return arguments[i];
      }
    }
  };
  // evaluate module specifications
  const specs = Function('return ' + source)();
  delete String.prototype.subclass;
  return specs;
}

// sequence of configure closures computes configuration
function collectModuleConfig(configureClosures) {
  const config = {};
  for (const closure of configureClosures) {
    closure(config);
  }
  return config;
}

// convert configured datatypes to flat typespace
function flatTypespace(definitions) {
  const flat = {};
  for (const name in definitions) {
    const source = definitions[name];
    flat[name] = typeof source === 'string' ? source : flatRecordType(source);
  }
  return flat;
}

// compute configured record type
function flatRecordType(fields) {
  const accu = [];
  if (fields.$macro) {
    accu.push('(');
    fields.$macro.forEach((equation, i) => { accu.push(i ? ',' : '', equation) });
    accu.push(')');
  }
  if (fields.$super) {
    accu.push(fields.$super, '+');
  }
  accu.push('{');
  let comma = '';
  for (const key in fields) {
    if (key.charAt(0) !== '$') {
      const source = fields[key];
      accu.push(comma, key, ':', typeof source === 'string' ? source : flatRecordType(source));
      comma = ',';
    }
  }
  accu.push('}');
  return accu.join('');
}
