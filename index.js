"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var pg = require("pg");
var pg_copy_streams_1 = require("pg-copy-streams");
var fs = require("fs");
var csv = require("csv-parse");
var stream_1 = require("stream");
var MIN_CONEXOES = 1;
var MAX_CONEXOES = 20;
var NOME_CSV = "./exemplo.csv";
function criarStreams(query) {
    return __awaiter(this, void 0, void 0, function () {
        var fileStream, copyStream;
        return __generator(this, function (_a) {
            fileStream = fs
                .createReadStream(NOME_CSV)
                .pipe(csv.parse({ delimiter: "," }));
            copyStream = function () { return (0, pg_copy_streams_1.from)(query); };
            return [2 /*return*/, { fileStream: fileStream, copyStream: copyStream }];
        });
    });
}
function criaConexao(opts) {
    if (opts === void 0) { opts = undefined; }
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new pg.Pool({
                    host: "ourobranco.ufsj.edu.br",
                    database: "test_migration",
                    user: "postgres",
                    password: "super",
                    min: MIN_CONEXOES,
                    max: MAX_CONEXOES,
                })];
        });
    });
}
function criarTasks(streams, pool) {
    return __awaiter(this, void 0, void 0, function () {
        var doneTasks, tasks;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    doneTasks = 0;
                    tasks = [];
                    return [4 /*yield*/, new Promise(function (resolvePipe, rejectPipe) {
                            streams.fileStream
                                .pipe(new stream_1.Writable({
                                objectMode: true,
                                write: function (line, _, callback) {
                                    tasks.push(function () { return __awaiter(_this, void 0, void 0, function () {
                                        var client, lineCsv_1, copyStream_1, err_1;
                                        return __generator(this, function (_a) {
                                            switch (_a.label) {
                                                case 0:
                                                    console.debug("aqui");
                                                    return [4 /*yield*/, pool.connect()];
                                                case 1:
                                                    client = _a.sent();
                                                    _a.label = 2;
                                                case 2:
                                                    _a.trys.push([2, 4, 5, 6]);
                                                    lineCsv_1 = line.join(",");
                                                    copyStream_1 = client.query(streams.copyStream());
                                                    return [4 /*yield*/, new Promise(function (resolve, reject) {
                                                            (0, stream_1.pipeline)(stream_1.Readable.from(lineCsv_1), copyStream_1, function (err) {
                                                                if (err) {
                                                                    console.error(err);
                                                                    reject(err);
                                                                }
                                                                resolve();
                                                                doneTasks++;
                                                                console.info("".concat(doneTasks, "/").concat(tasks.length));
                                                            });
                                                        })];
                                                case 3:
                                                    _a.sent();
                                                    return [3 /*break*/, 6];
                                                case 4:
                                                    err_1 = _a.sent();
                                                    console.error(err_1);
                                                    return [3 /*break*/, 6];
                                                case 5:
                                                    client.release();
                                                    return [7 /*endfinally*/];
                                                case 6: return [2 /*return*/];
                                            }
                                        });
                                    }); });
                                    callback();
                                },
                            }))
                                .on("finish", function () {
                                resolvePipe();
                            })
                                .on("error", function (err) {
                                rejectPipe(err);
                            });
                        })];
                case 1:
                    _a.sent();
                    return [2 /*return*/, tasks];
            }
        });
    });
}
function createTable(client) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, client.query("CREATE TABLE IF NOT EXISTS teste (id integer,fistname text,lastname text )")];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var pool, client, testeTableStreams, testeTasks, err_2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, criaConexao()];
                case 1:
                    pool = _a.sent();
                    _a.label = 2;
                case 2:
                    _a.trys.push([2, 8, , 9]);
                    return [4 /*yield*/, pool.connect()];
                case 3:
                    client = _a.sent();
                    return [4 /*yield*/, createTable(client)];
                case 4:
                    _a.sent();
                    console.info("Criando tabela");
                    client.release();
                    console.info("Lendo arquivo");
                    return [4 /*yield*/, criarStreams("COPY teste FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')")];
                case 5:
                    testeTableStreams = _a.sent();
                    return [4 /*yield*/, criarTasks(testeTableStreams, pool)];
                case 6:
                    testeTasks = _a.sent();
                    console.info("Inserindo");
                    console.debug(testeTasks);
                    return [4 /*yield*/, Promise.all(testeTasks.map(function (task) { return task(); }))];
                case 7:
                    _a.sent();
                    return [3 /*break*/, 9];
                case 8:
                    err_2 = _a.sent();
                    console.error(err_2);
                    return [3 /*break*/, 9];
                case 9: return [2 /*return*/];
            }
        });
    });
}
(function () { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, main()];
            case 1:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); })();
