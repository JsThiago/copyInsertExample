import * as pg from "pg";
import { CopyStreamQuery, from } from "pg-copy-streams";
import * as fs from "fs";
import * as csv from "csv-parse";
import { Writable, pipeline, Readable } from "stream";
const MIN_CONEXOES = 1;
const MAX_CONEXOES = 20;
const NOME_CSV = "./exemplo.csv";
const HOST = "";
const USER = "";
const PASSWORD = "";
const DATABASE = "";

type Streams = { fileStream: csv.Parser; copyStream: () => CopyStreamQuery };

/**
 * description
 * Cria as streams para serem inseridas no banco. Stream do arquivo lido e
 * da stream que insere no banco
 */
async function criarStreams(query: string): Promise<Streams> {
  const fileStream = fs
    .createReadStream(NOME_CSV)
    .pipe(csv.parse({ delimiter: "," }));
  const copyStream = () => from(query);
  return { fileStream, copyStream };
}
/**
 * description
 * Cria conexão com o banco
 */
async function criaConexao(opts: pg.PoolConfig | undefined = undefined) {
  return new pg.Pool(
    {
      host: HOST,
      database: DATABASE,
      user: USER,
      password: PASSWORD,
      min: MIN_CONEXOES,
      max: MAX_CONEXOES,
    } || opts
  );
}
/**
 * description
 * Cria as tasks de inserção que serão executadas em paralelo pelo banco de dados.
 */
async function criarTasks(
  streams: Streams,
  pool: pg.Pool
): Promise<Array<() => Promise<void>>> {
  let doneTasks = 0;
  const tasks: Array<() => Promise<void>> = [];
  await new Promise<void>((resolvePipe, rejectPipe) => {
    streams.fileStream
      .pipe(
        new Writable({
          objectMode: true,
          write: (line: Array<string>, _, callback) => {
            tasks.push(async () => {
              const client = await pool.connect();
              try {
                const lineCsv = line.join(",");
                const copyStream = client.query(streams.copyStream());
                await new Promise<void>((resolve, reject) => {
                  pipeline(Readable.from(lineCsv), copyStream, (err) => {
                    if (err) {
                      console.error(err);
                      reject(err);
                    }
                    resolve();
                    doneTasks++;
                    console.info(`${doneTasks}/${tasks.length}`);
                  });
                });
              } catch (err) {
                console.error(err);
              } finally {
                client.release();
              }
            });
            callback();
          },
        })
      )
      .on("finish", () => {
        resolvePipe();
      })
      .on("error", (err) => {
        rejectPipe(err);
      });
  });
  return tasks;
}

/**
 * description
 * Cria a tabela(deve ser de acordo com as tabelas presentes no arquivo csv)
 */
async function createTable(client: pg.PoolClient) {
  await client.query(
    "CREATE TABLE IF NOT EXISTS teste (id integer,fistname text,lastname text )"
  );
}

async function main() {
  const pool = await criaConexao();
  try {
    const client = await pool.connect();
    await createTable(client);
    console.info("Criando tabela");
    client.release();
    console.info("Lendo arquivo");
    // Para inserir em mais de uma tabela ao mesmo tempo deve-se criar
    // mais streams com a query desejada, criar as tasks e depois
    // colocar as tasks no promise.all(dar join entre os dois arrays)
    const testeTableStreams = await criarStreams(
      "COPY teste FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')"
    );

    const testeTasks = await criarTasks(testeTableStreams, pool);
    console.info("Inserindo");
    await Promise.all(testeTasks.map((task) => task()));
  } catch (err) {
    console.error(err);
  }
}

(async () => {
  await main();
})();
