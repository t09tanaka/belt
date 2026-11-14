# remote-task-runner 実装計画

`docs/spec.md`（仕様版 1.3）の実装を、PR 単位に分けた計画である。仕様の受入条件 A01〜A49 は、この表のいずれかの行が担当する。

## 前提

- 全 PR を `main` 宛てにし、単体で CI が通る形で都度マージする。依存列の「#N」は「#N が main にマージされた後、main から起点を切る」。
- 共通の検証コマンド（RTR = remote-task-runner）: `cargo fmt --all --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、GitHub Actions の緑。純粋モジュールは local-ci と同じく行カバレッジ 100% のゲートと cargo-schemata の変異テストに登録する。
- 略称: RTR = remote-task-runner、LC = local-ci、TTC = task-traffic-controller。

## 分割表

| # | repo | タスク | スコープ | 完了条件 | 非スコープ | 依存 |
|---|---|---|---|---|---|---|
| S0 | RTR | 仕様 v1.2 | `docs/spec.md`、`docs/plan.md`（この表と A 対応表） | 仕様 v1.2 を反映（workspace は fast-worktree、基準版を local-ci 0.9.0 / task-traffic-controller 0.5.0 へ、Linux と一時 worker、`.worktreeinclude`、A45〜A48）。公開禁止語の grep が 0 件 | コード | なし |
| S1 | RTR | crate の骨組みと CI | `Cargo.toml`（local-ci 0.9・task-traffic-controller 0.5 を crates.io から）、§18 の module を空で全部作る、clap で §4 の全サブコマンド（未実装は `NOT_IMPLEMENTED` の構造化 error）、`--json` の外枠（schema_version・error code）、`.github/workflows/ci.yml`（fmt・clippy・test を macOS と Linux）、coverage gate と cargo-schemata の枠 | 共通の検証コマンドが緑。`remote-task-runner <各サブコマンド> --json` が schema_version 付きの error を返すテスト | 各機能の中身 | なし |
| X1 | LC・TTC | core の不足調査（読むだけ・完了） | §16 の core API・§17 の公開 API と local-ci 0.9.0 / task-traffic-controller 0.5.0 の差分。結果は仕様 §16・§17 の「0.5.0 / 0.9.0 での状態」列に反映済み | 各 API が有る／名前違いで有る／無いと、必要な変更の一覧。T1・L1a・L1b・L2・L3 のスコープがこれで確定した | 実装 | なし |
| S2 | RTR | 契約（ID・状態・wire） | §10 の ID 型、状態機械と terminal outcome・観測状態・receipt、error code、§11 の全操作の request/response DTO（schema_version・digest） | 純粋モジュールとしてカバレッジ 100%・変異テスト。状態遷移表のテスト | 通信・保存の実装 | S1 |
| S3 | RTR | 設定 | §5 のユーザー設定・プロジェクト設定、優先順位、未知キーの error、`.local-ci.toml` の fallback と矛盾の出所表示、`[toolchain]`・`[source] secret_files`・`[[outputs]]` の型 | 設定例 2 つが通る、未知キー・矛盾のテスト | 設定の利用側 | S1 |
| S4 | RTR | storage | SQLite（WAL）、migration と互換表、親 DB・worker DB の schema、条件付き terminal 確定、event journal、retire tombstone の永続保持 | 未知 version を書き換えないテスト、terminal が 1 回だけ確定する競合テスト | 配置・通信 | S2 |
| S5 | RTR | protocol 転送層 | 長さ prefix JSON frame、`hello` の version・feature 照合、control を log が塞がない多重化、chunk 転送の offset・digest 再開、`worker --stdio` の Unix socket bridge、SSH 起動（argv をシェル文字列にしない） | 切断・再開・非互換 version のテスト | 各操作の業務処理 | S2 |
| S6 | RTR | source capture と object store | `local_ci::capture_source` を使った worktree / commit / directory、private object store、§6 の SourceManifest、SOURCE_BUSY、未 push objects の bundle、submodule・LFS、展開時の path 安全性 | A11〜A14 相当のテスト（一時 Git repo の自作 fixture） | worker での展開 | S2（X1 で capture に不足があれば L 行も） |
| S7 | RTR | worker の workspace | repo ごとの canonical clone、attempt workspace を fast-worktree で作る・消す（プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` をそのまま使う）、directory snapshot、`exec --in-place`（親 local worker 限定） | macOS と Linux で add / remove が通るテスト、元 worktree を書き換えないテスト（A48） | setup template（S17） | S6 |
| S8 | RTR | secret_files | §22.3 の SecretBundle（cluster 鍵の HMAC）、親での 0600 保持と削除、転送操作、0600 配置、terminal 時と reconcile 時の消去、ログの mask | A43・A47 のテスト | toolchain | S5・S6 |
| S9a | RTR | supervisor（寿命・lease・停止） | attempt ごとの別プロセス、task-traffic-controller の managed `register` / `try_admit` / `run_command`、launch intent、process identity（boot ID・開始時刻）、suspend を含む単調時計の lease、renew の冪等性、cancel の TERM→KILL と停止確認、3 種の timeout、実行中の sleep 抑止 | A20・A21・A22・A25・A26（時計）のテスト。OS 依存部分（単調時計・boot ID・process identity・sleep 抑止）の macOS 版と Linux 版（A45） | log spool（S9b） | S4 |
| S9b | RTR | log spool と結果記録 | stdout / stderr の disk spool、stream ID・offset・sequence、非 UTF-8、上限 1 GiB と `log_truncated`、結果と artifact の attempt 別保存 | A29 のテスト | 回収（S14） | S4 |
| S10 | RTR | worker agent | §11 の worker 側操作すべて、`attempt_id + payload_hash` の冪等受理、retire tombstone、再起動時の supervisor adopt、boot ID 変化で interrupted、`ack_collected` までの GC 保護 | A18・A19・A23・A24（擬似）のテスト | forecast（S11）・導入（S22） | S4・S5・S9a・S9b |
| S11 | RTR | worker forecast | task-traffic-controller の forecast に受理済み未登録の overlay を重ねる、coverage（attempt・plan_digest・expansion_seq・node）、読み取り専用、TTL と epoch 失効、measured / fallback / unknown | A06〜A10 のテスト | 配置の判断（S13） | S10・X1 |
| S12 | RTR | coordinator の中核 | durable queue（run・task・attempt）、prefetch / max_active を守る dispatch、未 ACK 需要を同じ transaction に記録、retire してから再配置、unknown の保持、epoch と再起動時の reconcile・adopt、cancel の伝達 | A19・A21・A22・A23 のテスト（親側） | 予測による選択（S13）・回収（S14） | S4・S5・S10 |
| S13 | RTR | scheduler | 1 actor で配置を直列化、転送＋setup＋実行＋回収の予測完了時間で比較、同点規則、途中参加 worker の取り込み、配置理由の表示 | A01〜A04 のテスト（1 台上の複数 worker） | CI 固有の処理 | S11・S12 |
| S14 | RTR | 結果の回収 | collecting 状態、artifact の digest 検証と atomic 公開、`ack_collected`、回収だけの再開、完全性検査と `no_work`、保持 7 日の GC | A30・A31 のテスト | GitHub status（S19） | S9b・S12 |
| S15 | RTR | 利用者コマンド | `init`・`exec`・`submit`・`ls`・`show`・`logs --follow`・`wait`・`cancel`（Ctrl-C）・`retry`・`outputs`・`estimate`、JSON と exit code | 各コマンドの統合テスト、`exec` の exit code 透過 | `ci`（S16）・`workers`（S21〜S23） | S12・S14 |
| S16 | RTR | CI 統合 | `ci` / `ci plan`、FrozenPlan を 1 回だけ作る、Job DAG の配置と needs output、dynamic matrix の展開 event、host をまたぐ中間 artifact の receipt（workflow 単位）、matrix の global permit・max-parallel・fail-fast・join・決定的な集約、分割できない理由の表示 | A10・A27・A28・A38 のテスト | test sharding（S18） | S13・S14・L1a・L1b |
| S17 | RTR | setup と template | SetupRecipe / PreparedTemplate receipt、worker 内 singleflight の builder（builder_id・consumer 参照・独自 lease）、immutable template と書き込み分離の複製、`--no-cache`、`[prepare]`。fast-worktree の `[[clone]]` との境界は仕様 v1.2 に従う | A15〜A17 のテスト | toolchain の導入（S20） | S7・S9a・S10 |
| L1a | LC | step 実行の差し込み口 | `exec::Opts` に `CommandExecutor` 相当の trait（`StepRequest` を受けて `StepExit` を返す）を足す。executor が無いときは従来どおりバイナリを子プロセスで起動する。pipe fd（token の受け渡し）と process group の扱いを trait の契約に明記する。local-ci は task-traffic-controller の crate に依存しない | local-ci の CI（coverage・変異）緑、executor 未設定の挙動がバイト単位で不変（A36）、`cargo publish` まで | L1b・L2・L3 | なし |
| L1b | LC | task demand 列挙と wire DTO | `plan_task_demand`（task-traffic-controller の型に依存しない DTO。条件式・動的 matrix は `conditional`）、`SetupRecipe`・`PreparedReceipt`・`TaskResult`・`TaskInputs`・`ExecutionTask`・`JobGraph` の schema_version と digest、wire から host path を除く（`StepResult.log`・`RunResult.run_dir`）、`RunResult` の `Deserialize` | local-ci の CI 緑、既存 CLI の JSON 出力は不変（A36）、`cargo publish` まで | test sharding（L2・L3） | L1a・L2 |
| L2 | LC | test sharding（土台＋cargo test・Vitest・Jest） | `partition_tests`、adapter の discover / partition / invoke / collect / merge、履歴による長い順の割り当てと決定的な fallback、全件 1 回の集合検証 | A39・A40 のテスト（同梱 example） | Playwright・Cypress・coverage merge | X1 |
| L3 | LC | test sharding（Playwright・Cypress・coverage merge・分割禁止条件） | 2 adapter、runner 対応の coverage merge、DB / port / container を隔離できない E2E の分割禁止 | A41 のテスト、`cargo publish` まで | RTR 側の配置 | L2 |
| T1 | TTC | 分散用 API の不足分 | `exec::Opts` にenv map（`env_clear` 後に設定）とstdio（stdout・stderrの追記先log path、stdinはnull）を足し、`managed::run_command` まで通す。`None` 既定で既存CLIの挙動を変えず、履歴keyにenvを混ぜない。任意: `legacy_clock`、inspect の history quality | task-traffic-controller の CI 緑、env 隔離・log 追記・lease と marker が残るテスト、既存 CLI の挙動不変、`cargo publish` まで | RTR 側 | なし |
| S18 | RTR | sharding の配置 | ShardGroup の配置と集約、fail-fast、失敗 Shard の retry、通過済み結果の保持 | A39〜A41 の統合テスト | adapter 本体 | S16・L3 |
| S19 | RTR | GitHub status | 親だけが投稿、公開 commit と検査入力の一致検証、dirty / synthetic は success にしない、計算結果と投稿失敗を分けて報告 | A32 のテスト | local-ci 単体の status の変更（必要なら L 行を追加） | S16 |
| S20 | RTR | toolchain | §22.1 の版指定の読み取り、baseline、match 規則、adapter（nodenv・rustup・mise・fvm・corepack）の detect / resolve / install / activate、setup と同じ経路の導入、`blocked: environment_mismatch` | A33・A42 のテスト | diff / sync（S21） | S10・S17 |
| S21 | RTR | `workers diff` / `sync` | §22.2 の差分項目と 5 分類、sync は自動導入できる差だけ、`doctor` へ含める | A44 のテスト | 導入・常駐（S22） | S20・S22 |
| S22 | RTR | 導入と常駐（macOS） | `workers add --install`（SSH bootstrap、版別ディレクトリ）、LaunchAgent / LaunchDaemon（`UserName`）、start / stop / drain / resume / `stop --cancel-running` | 1 台上での install → start → stop の統合テスト | upgrade・doctor（S23）・Mac 以外（S24） | S10 |
| S23 | RTR | upgrade・rollback・doctor | drain → 配置 → 互換検査 → 切替 → health → resume、失敗時の rollback、doctor（task-traffic-controller runtime の互換・policy digest・state path・Docker の resource domain・service 環境での profile 検査） | A05・A34・A36 のテスト | Mac 以外 | S22 |
| S24 | RTR | Linux の常駐と Colab の一時 worker | Linux の systemd user service（install / start / stop / upgrade）、Colab の一時 worker（`worker --ephemeral` をノートブックから起動、利用者が用意したトンネル経由で親が SSH、起動ごとに新しい identity、`workers forget` で消滅を宣言したときだけ interrupted に確定） | Linux（CI の ubuntu）での install → start → stop の統合テスト、ephemeral の identity と forget のテスト | macOS の常駐（S22）、トンネルの提供 | S22・S23 |
| S25 | RTR | 1 台上の試験ハーネスと障害注入 | 複数 worker（別 state dir）を 1 台で立てる、kill 点・ACK 消失・時計のずれの注入口、A 対応表のうち実機不要のものを自動試験にまとめる | 実機不要の A がすべて自動で緑 | 実機の試験（S26） | S15・S16・S18・S23 |
| S26 | RTR | Docker での受入と初回リリース | 手元の Mac と Docker の複数 Linux worker で A01・A02・A24・A26・A34・A35・A37・A45・A46 の適用可能な範囲を検証、結果の報告、0.1.0 の `cargo publish` とタグ・Release | 適用可能な A が緑、§20 の完了報告。独立 Mac 固有の未検証項目は明記する | 追加 Mac の調達 | 全行、Docker。受入環境は下記に従う |

## 現在の進捗と追加作業（2026-11-14）

この計画を進捗と追加変更の基準とする。完成に必要な不足が判明したら、理由・担当・検証条件・依存を追記する。実装済みの部品と、利用者が実際に使う経路への接続は区別し、接続と受入検証が終わるまで担当行を完了にしない。

- S0〜S14 と S17（PR #38、merge `9a9abc8`）は main に統合済み。S17 の検証対象 `279449b` はローカル実行 9 ジョブとリモート 9 ジョブが成功。S15 以降の接続と、追加検証で見つかった回復・テストの問題を別変更で進める。
- S15 は以下の小さい PR に分ける。S15a は S17 の統合結果へ載せ直し、PR #40 で本流へ統合済み。S15b の library 常駐基盤も実装・独立レビュー済みであり、別 process の bootstrap と CLI 接続は S15c で検証する。独立した修正・調査は担当を分け、設計と統合は親担当が行う。
- 検証はコミット済み HEAD に対する `local-ci run --include-disabled` を基準とし、対象 SHA、コードの失敗、環境エラー、未再現の範囲、リモート CI の結果を分ける。共有ツールが原因なら、そのツールで修正・検証・公開した後、RTR の利用版を別変更で追従する。

| 追加行 | 担当 | 必要になった理由・スコープ | 完了条件 | 依存・状態 |
|---|---|---|---|---|
| F1 | TTC | 同じ秒に開始した無関係な session leader が、孤児の候補から外れない。同 boot の単調時刻で子より古いと証明できる場合だけ除外する | 修正前に失敗する決定的な回帰テスト。unknown・equal・newer・別 boot・矛盾する証拠は保持。関連統合テストと CI、修正版の公開 | PR #29 統合、0.8.1 公開済み。ローカル実行ジョブ 5 件・リモート 5 件成功。ローカルの installer action 未再現は別記 |
| F2 | fast-worktree | 並行する Git worktree 登録が、別の登録途中の空 `commondir` を読む競合 | Git ネイティブの登録操作と関連する読取・削除を repository 共通の排他で保護。hook は抑止せず、同 repo への同期再入は lock を迂回せず即時エラーにして deadlock を防ぐ。並行登録・hook の実行と拒否・再入・失敗時の状態の回帰テストと CI、制限を明記した修正版の公開 | PR #11 統合、0.4.0 公開済み。ローカル 3 件・リモート 3 件成功。native Git hook 中は同 repo の操作が直列化される。依存準備と fast-worktree の設定 hook は排他外。独自の排他を使わない raw Git は保護範囲外 |
| F3 | RTR | 修正済み共有ツールと実行環境への追従 | TTC・fast-worktree の修正版を利用し、必要版・表示・テスト・文書を一致させる。local-ci の実行失敗を解消し、PR #38 をマージ | F1・F2 完了、PR #38 統合済み。追加の型付き回復 API は F8 で扱う |
| F4 | 検証環境 | CI の Rust 1.100 と古いローカル formatter・coverage ツールの差 | 同じ Rust 版と対応する coverage ツールで検証。新しい compiler で判明した未カバー分岐をテストし、ゲートを維持 | formatter と forecast の不足テストは PR #38 に反映。coverage ツールを CI と同版へ更新済み |
| F5 | RTR | protocol flood のテストが無受信 1 秒を全 request 到着とみなし、遅い RPC を取り残す | 全 caller の応答と受理数を照合して終了。RPC が同期の各 phase より先に timeout しない階層予算を持ち、遅延到着を sleep なしで再現する回帰テストを追加 | 関連 3 テストを各 20 回、計 60 回成功。HEAD `279449b` の全体ローカル実行ジョブ成功。Overloaded/Internal の分類と件数の検査は維持 |
| F6 | RTR CI | 同一ホストの mutation 4 shard がそれぞれ多数の Rust/Tokio integration test を並列起動し、基準実行の lease・deadline 検証を過負荷にする | mutation job に `RUST_TEST_THREADS=2` を設定し、4 shard・全テスト・全 mutant・判定基準は維持。cargo-schemata の基準実行と mutant 実行への継承を確認 | 既知失敗 5 件×4 プロセスと、関連 module 114 件×4 プロセス、計 456 件の module 検証が成功。HEAD `279449b` の全体ローカル実行 9 ジョブ成功。導入 action 未対応による incomplete は保持。リモート 9 ジョブも成功 |

| F7 | RTR test | 稼働中 SQLite DB と SHM を同一 process で raw open/close すると POSIX lock が解除され、別 writer 終了時の WAL 消失と readonly エラー 522 を起こす | 独立 process で DB/WAL/SHM の物理バイトを検査し、論理削除済みデータの漏洩検査も維持する。readonly 接続を弱めず、最小再現と WAL 回帰で検証 | 通常 cleanup の関連 3 件と追加回帰 4 件が成功。独立 process の raw scan で論理削除済み WAL の物理残存も検査し、PR #38 は統合済み。VACUUM による圧縮 snapshot は検査範囲が狭くなるため採用しない |

| F8 | TTC・RTR | 回復時の TTC `ProbeUnknown` を通常の失敗に数え、identity の不確定と他の失敗が混在した場合も分類できていなかった | 同じ ledger lock 内の解決判定を型付き API で返し、文字列解析・再 scan を避ける。ProbeUnknown だけの不確定は再確認し、混在原因は区別する。新 launch や偽 terminal を作らず回復する決定的テストと全 CI | TTC PR #30 の観測同期修正と PR #31 の API を統合し 0.8.2 公開済み。ローカル・リモート各 5 ジョブ成功、配布 8 asset の checksum 一致。RTR の関連 unit 31 件・実回復 1 件・pure mutation 2 caught 成功、RTR `623cfd5` のローカル実行 9 ジョブとリモート 9 ジョブ成功、PR #39 を merge `3270420` で統合済み。負荷下の既存時間依存テストは別件として残し、未証明の CPU guard 設定案は採用しない |

F2 は登録と hook を分離する案も調査したが、`core.hooksPath` の抑止は reference-transaction hook まで消し、`--no-checkout` でも同 hook が登録中に発火する。hook の契約を削らず単純な排他を採用するため、長い native hook の並行性と同期再入の制限を明示する方針へ変更した。Git の最低版を上げる案は採用しない。

### S15 の実装順

| 行 | スコープ | 完了条件 | 依存・状態 |
|---|---|---|---|
| S15a | 設定から Coordinator・Scheduler・Agent・Setup への checked 変換、検証済み worker identity と priority、`TaskSpec.setup` の転送、`Machine::with_setup` 必須の共通 factory | 非既定値・単位変換の境界・lease/budget/TTL・identity の不一致・旧 task JSON・setup payload 転送・factory の関連テスト | PR #40 を merge `8939ef25c877448f3ae428caf6f666bebd6afad3` で統合済み。対象 `87f8df2` は Rust 1.100 のローカル実行 9 ジョブ成功（native installers は未検証）、リモート 9 ジョブ成功（Linux・macOS・MSRV 1.89） |
| S15b | 親の常駐 loop、local worker、hello 検証済み remote link、再接続、独立した回収 loop、単一の ResultStore、ローカル IPC | 呼出元 CLI 終了後も durable task が実行・回収され、再起動で同じ attempt を引き継ぐ。遅い回収が lease 更新を止めない | S15a |
| S15c | `init`・source/secret capture と durable `submit`・`ls`・`show`、run/task の問い合わせ API | 同時 CLI でも coordinator epoch が増殖しない。capture 失敗時は配置せず、完了済み run も正しく列挙する。JSON と stderr の契約を検証 | S15b、起動方式は S22 と整合 |
| S15d | `exec`・`wait`・`cancel`・Ctrl-C・明示 `retry`・local-only `--in-place` | exit code を透過し、停止不明を cancelled にしない。unknown の retry を拒否し、元 snapshot と履歴を保つ。in-place を remote に配置しない | S15c |
| S15e | `logs --follow`・再接続・`outputs`・retention の常駐 loop への接続 | 非 UTF-8・offset・切断再開、出力 path 衝突と部分失敗、回収失敗、実行中・回収中・保持対象を消さない GC を検証。自動削除に件数上限と時計跳躍の保護を付ける | S15d・S14 |
| S15f | 読み取り専用 `estimate`、配置理由、転送・setup・回収の実測値の接続 | estimate が queue・attempt・ledger 需要を増やさない。measured/fallback/unknown・TTL・priority の説明を保つ。S15 の受入条件を利用者経路で閉じる | S15b〜S15e、S11・S13 |

S15c の常駐入力は既存 user config の隣にある private `coordinator.json` に保存し、既存 bootstrap input を再利用する。`config.toml` の文法や暗黙の環境変数は追加しない。未初期化は明示エラーとし、`ls` / `show` は停止中でも読み取り専用 DB を使い、起動・epoch 更新・migration を行わない。`submit` だけが必要な owner 起動を要求する。

S15c の残件には、file/reader ベースのメモリ上限付き source 転送、active attempt のページ走査、requires/profile の配置条件、success/failure/always と glob・複数 path の出力計画を含める。現在の限定的な submit 経路でこれらを明示拒否していても、完成条件からは外さない。終了済み worker の spec・workspace は回収 ACK 前にも削除し、保持対象の結果・ログは残す。親側 source/Vault の retention は S15e で retry と回収条件を保護して接続する。

S15c は source/secret capture、CLI 終了後も動く local durable submit、bounded な offline query を実装・独立レビュー済み。blob/bundle は同じ検証済み FD から最大 64 KiB ずつ送り、転送停止時は owner を保持せず専用 lease で残る IO と cleanup を保護する。関連 7 件・strict clippy が成功。統合差分の全体検証をこれから行い、remote worker の lifecycle は未完了として残す。

S15d1 は `exec` / `wait` / `cancel` を先に接続する。run cancellation intent の migration、全 task 作成・attempt 作成・配置入口への取消 gate、定数メモリの completion query、純粋な終了判定と CLI 接続を実装した。pure module は行カバレッジ 100% と実変異テスト（43 caught・survivor 0）を確認し、取消 race・exit code・受理前 Ctrl-C・wait detach・readonly の project/Git 非依存を focused 回帰で確認した。owner 再起動の wait は並行実行時の 20 秒 pending timeout と単独実行の成功があり、原因未確定として保持する。instrumented CLI の profile 出力先を fixture の子 process へ継承し、元条件の最終 focused・immutable HEAD の全体 CI を待つ。候補の CPU guard 設定は最終差分から除外した。受理前 Ctrl-C でも取消が失われないよう durable run cancellation intent を enqueue transaction で反映する。取消受理と停止確定、計算終了と回収成功・回収閉鎖を区別する。明示 retry は仕様どおり同 run の新 attempt とし、取消 intent を消して古い投入を復活させない generation authorization を次の slice で設計する。`--in-place` も必須残件として維持する。

S15d2 は same-run retry を独立 slice とする。元 TaskSpec・source・run Vault と attempt/collection の履歴を保ち、run 対象は成功 task を保持して不成功 task を再実行する。task 対象は成功を含む known terminal attempt だけを明示再実行する。unknown/live・停止未確認・closed run・未配置の取消・失われた元入力は変更なしで拒否し、消去済み bundle は SecretUnavailable とする。現在の作業ツリーや secret の再 capture、無期限保持は行わない。

fresh Cancel/Retry の両方が run の control revision を 1 増やす。request_id・operation・target・expected revision を control_request に保存し、同じ再送には元の bounded Cancel Snapshot/compact Retry receipt を返す。異なる kind/target/expected への ID 再利用は拒否する。これにより、古い Cancel が新 Retry に届く順序と、古い Retry が新 Cancel の後に届く順序を両方 fence する。受理前取消の revision は初回 run INSERT でも tombstone から継承する。内部の fresh cancel_run/cancel_task も同じ transaction を使い、worker 向け request_cancel delivery は revision を増やさない。client は request ID/expected を再送で固定し、自動的な revision 再取得はしない。cancel の停止待ちは新 revision を元の取消の停止確認として扱わない。

入力の source/Vault preflight は 1 run につき 1 回、DB writer の外で共有 transfer lease 下に行い、commit 直前に revision・元 manifest/secret binding を再確認する。task は task_id keyset 64件ずつ読み終え、statement/rows を破棄してから同 page を更新する。receipt は run 規模の task 一覧を持たず一定サイズに保つ。停止時の残存 IO は入力と cross-process lease だけを保持し owner を解放する。bounded status も queued retry 中に旧 attempt を current として返さない。将来の source/Vault GC も同じ lease を取得し、その後で現 generation の terminal/required collection 条件を再確認してから削除する。

focused storage 85件、resident unit 5件、実 CLI の元 snapshot 再利用・owner 再起動・Cancel ACK/Retry receipt 再送、保持/消去済み secret、readonly/受理前取消、resident 統合6件と strict clippy が成功した。既存 owner 再起動テストは resident 群の並行実行で lock 残存が 1 回あり、base 87bf85e と変更版の同名 focused 各3回では再現せず、未帰属としてログを保持した。assertion の弱化や shutdown 修正はこの slice に混ぜない。`--in-place` と親側 retention の常駐接続は別 slice の必須残件として維持する。

S22 の native 所有確認は、Apple 標準 service の状態を読み取り専用で調べた実際の形式に基づいて実装する。print は安定 API ではないため未知・曖昧な形式は拒否する。GUI domain、system-user の名前と UID、実行 argv と managed file receipt、認証済み worker を照合し、直前にも再確認する。cooperative lifecycle lock による通常の取り違え防止を目的とし、OS に無い原子的 CAS や同 UID/root の任意差替え防止を保証したとは記載しない。テストは fake と private temp dir のみで、実 launchd の変更は行わない。

S16 は、S16a（入力確定済み FrozenPlan の再利用と安全な summary adapter）、S16b（S15c と共有する source 確定から公開 `ci plan` への接続）、その後の DAG 実行・動的 matrix・permit・artifact 集約の順に進める。S16a の時点で CLI の既定 snapshot や未実装状態は変えず、公開 `ci plan` 完成とは扱わない。

S16・S18〜S25 は元の分割表の必須範囲を維持する。新しい不足はこの追加表へ登録し、未実装の必須項目を「将来対応」として完成から外さない。

### 受入環境と S26 の進め方

- 当面の追加 worker と Linux 検証には Docker を使う。別 state dir と portool で分離し、複数 worker、親の再起動、接続断、ACK 消失、worker 消失、cold/warm と親単独との差を確認する。
- Docker 上の Linux worker と macOS ホストの結果は区別する。Docker を別の Mac 実機と記載せず、独立した Mac の sleep・boot・launchd・実機間転送など、代替できない項目は実機確認待ちとして A 対応表と最終報告へ残す。
- 公開する版には Docker で確認した範囲と未検証範囲を明記する。実機未確認の項目を、A01〜A49 全件確認済みと報告しない。

## A01〜A49 の対応

| A | 担当行 | 実機要 |
|---|---|---|
| A01〜A04 | S13（A01 は S26 でも） | A01・A02 |
| A05 | S9a・S23 | |
| A06〜A10 | S11（A10 は S16 も） | |
| A11〜A14 | S6・S7（A13 は S5 も） | |
| A15〜A17 | S17 | |
| A18〜A19 | S10・S12 | |
| A20〜A23 | S9a・S10・S12 | |
| A24 | S10（擬似）・S26 | ○ |
| A25 | S9a | |
| A26 | S9a・S26 | ○ |
| A27・A28・A38 | S16 | |
| A29 | S9b・S5 | |
| A30・A31 | S14 | |
| A32 | S19 | |
| A33 | S20・S8 | |
| A34・A35 | S22・S23・S26 | ○ |
| A36 | L1a・L1b・L2・L3・T1・S23 | |
| A37 | S26 | ○ |
| A39〜A41 | L2・L3・S18 | |
| A42 | S20 | |
| A43 | S8 | |
| A44 | S21 | |
| A45 | S9a・S10（OS 依存部分）・S24 | ○（S26 の Linux） |
| A46 | S24・S26 | ○ |
| A47 | S8 | |
| A48 | S7 | |
| A49 | S13（配置）・S27（設定） | |

## 確定事項（仕様 v1.2 に反映済み）

- Linux（x86_64・arm64）を正式対応する（systemd user service）。Google Colab は常駐なしの一時 worker とする。OS 依存部分（suspend を含む単調時計、boot ID、process identity、sleep 抑止）は platform module に macOS と Linux の実装を置き、S9a・S10 から両方で動かす。
- `.worktreeinclude` に一致する ignored ファイルは、自動で secret_files と同じ経路で送る。`secret_files` には追加分だけを書く。
- 追加実機が未準備のため、当面は Docker の複数 worker と Linux・障害注入で受入を進める。Mac 実機固有の未検証項目は対応表に残す。

### S15 public init integration

Mac user init wires one LaunchAgent to the foreground combined coordinator/local-worker resident, using private retained input, a receipt journal, hash-addressed binary and authenticated connect-only readiness. Initialization never starts a detached fallback. Journaled pending publication binds device/inode/digest before publication; foreign or uncertain state requires reconciliation. Ordinary init retains its pinned binary and requires the separate upgrade flow for changed bytes. Native launchd acceptance is still required: fake transport and owned-child tests do not establish real launchd behavior. Linux public init requires S24 systemd backend and currently refuses before filesystem changes. System-user installation, runtime WorkerControl stop/drain/resume, SSH remote enrollment and upgrade/rollback remain required S22/S24 dependencies, not completion exclusions.

Managed init binds planned cluster/coordinator IDs before preparing a staged metadata database at epoch 0; the first daemon claim advances it to epoch 1. A private optional managed-service reference in the immutable input makes normal bootstrap connect-only, validates journal/input/key/database receipts and refuses incomplete or foreign bindings. Readiness compares authenticated resident PID/start/boot identity with fresh running launchd observations. Stage parent directories and newly created directory ancestry are synced before a pending journal may reference them; uncertain journal/publication errors retain the exact staged inode for reconciliation. Legacy unmanaged bootstrap keeps its prior behavior.


同時 init が `init.lock` 作成前の directory 作成で競合し、`AlreadyExists` をエラーにする問題を追加検証で確認した。競合した entry は非 symlink の実 directory と再確認し、勝った側だけでなく競合した側でも directory と親を fsync する。private leaf の所有 UID・permission 検査は維持する。directory/file/symlink と fsync の決定的 unit、および同じ epoch・worker・cluster と 1 回だけの daemon 起動を確認する所有子 process の同時 init テストが成功。負荷下の後者は約 120 秒を要した。strict all-target clippy・fmt・diff 検査も成功し、最終 immutable HEAD の全体 CI は親担当で行う。

Standalone worker foreground slice: `worker --input` reads an owner-only retained
WorkerSpec (schema 1, enrolled cluster/worker IDs, pinned executable, private key,
allowlisted environment and captured user TOML). Its state must be the canonical
HOME worker state so `worker --stdio` reaches the same agent socket. This is a
persistent role; `--ephemeral` remains a separate required S24 implementation.
The runtime owns its agent lock, independent tick/supervision and staged socket
publication with durable inode receipt. Bridge disconnection never shuts it down.
Only a dead socket matching the private receipt is reclaimed after a crash;
foreign or uncertain ownership is refused. Unpublished crash staging directories
are retained for future owned-artifact reconciliation. Cooperative owner locks and
inode rechecks do not provide atomic protection against arbitrary same-UID/root
replacement. Provisioning must retain this input/identity and the canonical HOME
binding when native worker services are added. This slice does not complete S22:
workers add/install, SSH bootstrap/key enrollment, versioned deployment,
launchd/system-user and systemd worker roles, upgrade/rollback, worker lifecycle
controls and native platform acceptance remain required. S24 ephemeral identity
and forget semantics also remain required.
Native worker service plans must select the worker foreground role rather than
reuse the combined parent's `daemon --input` role. Parent remote-link attachment
and per-worker source/secret routing are separate required slices: the existing
resident source and secret delivery paths still target its local worker. A public
standalone worker does not by itself establish remote end-to-end execution.


### Resident source-ready gate slice

`SubmitCaptured` の `Accepted` は run/task が durable queue に記録された意味とし、worker の source 受領や `WorkerAccepted` receipt とは区別する。local resident も source IO を独立した bounded background job へ移し、実際の worker `BlobVerify.ready` 後だけ new placement を許可する。接続ごとの volatile cache は最大 64 run とし、run/source/manifest digest と connection generation が一致した forecast だけを commit に使う。queued/live の metadata scan と secret delivery は keyset page、eligible run/worker union は SQLite `json_each` を使う。未評価 cache entry は保持し、dispatch 評価済み entry は FIFO eviction で後続 run へ進む。gated dispatch も keyset cursor で同 run 内の大きな backlog を走査し、filtered page で既存 waiting state を消さない。

source export/upload の共有 transfer lease と cleanup は background job 間で維持する。admission は network や transfer mutex を待たない。停止後の残 IO は capture store と lease だけを保持し、旧 coordinator owner を保持しない。secrets は既存 attempt/generation へ配置後に送り、worker の Preparing/lease gate を維持する。local に事前配置した legacy source も再起動後に worker verification で Ready を再構築する。

この slice は local resident と再利用できる per-worker delivery helper まで。verified remote の SSH 接続・再接続 owner、worker add/install・鍵登録・native service、実 remote source/secret routing と SSH/native end-to-end acceptance は次 slice の必須残件で、S15b 以降と S22 の完了を意味しない。schema migration は不要。

### S15e の増分ログ回収

S15e1a は schema v7 の stream ごとの committed offset、GC ref の先行登録、CAS 更新、未 commit tail の回復、読取専用の prefix reader、検証済み旧 receipt の引継ぎを実装・独立レビュー済み。storage 92 件・ResultStore 14 件・migration と読取専用 CLI の関連 1 件、strict clippy が成功した。collector / resident の増分回収と公開 `logs --follow` は後続 slice で接続する。`outputs`、resident GC と reader lifetime の接続は別の必須残件であり、この基盤だけで S15e 完成とは扱わない。

S15a は PR #40（merge `8939ef2`）、S15b は PR #41（merge `05e981c`）で main に統合済み。各対象 HEAD のローカル実行 9 ジョブとリモート 9 ジョブが成功した。S15c の bootstrap / source / query slice は PR #42 でリモート CI を確認中。


S15e public logs slice は read-only metadata / committed-prefix reader を公開 `logs` と
`logs --follow` へ接続する。32-task pagination、64 KiB raw chunk、NDJSON base64、
private SQLite 配信 cursor、latest/min_generation と明示 --attempt、collection closed
までの follow と Ctrl-C detach を実装する。active collector から公開 observer までの
回帰、raw bytes / retry / 欠損 / 大規模ページ / cursor permissions を focused 検証済み。
unit 10 件・active collector から公開 follow まで 1 件・実 CLI 2 件が成功し、
実行中および終了後 final の blocked stdout でも SIGINT detach と cursor cleanup を確認した。
strict all-target clippy、fmt、diff check も成功した。
outputs、resident GC / reader lifetime、output planner、estimate は別の必須残件として残す。


S15d in-place slice は公開 `exec --in-place` を既存の型付きWorkspaceへ接続する。
Git worktree / directoryの原本rootをprivate run bindingに保持し、baseline sourceと
mutable modeを分離する。親local identityのscheduler制限とworker admissionの両方で
remote配置を拒否し、setup template/cacheと自動retryを無効化する。明示retryはD2の
停止・回収・revision条件を維持して同じ原本を再実行する。元secret inventoryの増減は
新しいexecを求め、値変更は現物をread-onlyでmaskする。原本へのsecret配置・消去を
行わない。public task/completionはpathを含めずin_place flagを示す。private bindingは
親stateとともに保持し、個別GCは後続の親retention統合事項とする。

このsliceのfocused検証はin-place unit 5件・integration 8件（実CLIの原本更新、
owner再起動と明示retry、Git secret現物maskとinventory拒否、prepareとrequired output、
SIGINT停止確定、scheduler / worker admission / 原本cleanup）に加え、secret unit 76件、
retry関連20件、legacy payload hash 1件、既存実SIGINT 1件、未実装CLI grammar 3件が成功。
strict all-target / all-feature clippy、fmt、diff checkが成功。全体CIと公開は統合時に行う。

独立レビューのP2（元subdirectoryが外部symlinkへ差し替わると明示retryでroot外へ
cwdが解決される）を修正した。Agentのprepareでin-placeの全command cwdを
canonicalizeし、directoryかつ検証済み原本root内の場合のみresolved pathを使う。
owner再起動後の差替えretryがrunning前に拒否され、外部markerを作らず原本とsecretを
保つ回帰を含むin-place unit 5件・integration 9件、通常snapshot exec回帰、strict clippy
が成功した。TTCのpath登録後に同UIDが任意に差し替える競合への新たなFD layerは追加していない。

### 結果 retention GC の独立レビュー修正

独立レビューの 3 指摘（祖先 symlink 経由の外部削除、GC pin 競合時の live collection 全体停止、run 選択での task 全件集計）に限定して修正する。
root/祖先 FD と NOFOLLOW による unlink、競合対象の pending 再試行、run ID index の 1 件 keyset query を実装した。
実 GC claim と外部 sentinel、root/祖先/leaf symlink と rename 競合、pin 競合中の収集/tick と実 resident の IPC/lease 継続・解放後の回収再開、破損 pin、2000 task と EXPLAIN/no-task-facts の回帰を追加した。
修正後の results unit 39 件、guarded retention 4 件、indexed run 選択 1 件、実 resident の競合回帰 1 件、collection 競合回帰 2 件、strict clippy all-targets/all-features は成功。collection 全体は 38 件中 37 件成功後、新規 fixture の pin backfill 不足を修正し、その 2 件を再実行して成功した。
独立レビュアーの resolved 確認、全 CI と Linux 実行はこの実装修正時点では未完了。pure clock policy は変更していない。

追加調査で enqueue の run INSERT が create_run の durable pin 準備を迂回していたため、両公開経路を確認し、enqueue も既存 blocking wrapper 内・DB transaction 前に pin を作成/fsync するよう修正した。
pin 準備失敗時に run/task が公開されない回帰と、実 resident IPC の submit を全 worker offline で受理した直後に logs 共通 observer が pending を返し follow を継続する回帰が成功した。追加修正後の strict clippy all-targets/all-features も成功。独立レビューの resolved 確認と全 CI は引き続き未完了。
