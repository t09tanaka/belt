# remote-task-runner 実装計画

`docs/spec.md`（仕様版 1.3）の実装を、PR 単位に分けた計画である。仕様の受入条件 A01〜A49 は、この表のいずれかの行が担当する。

## 前提

- lease 更新の実プロセス試験は、準備開始・更新の durable commit・seed の更新キーと単一 monotonic 測定期限・コマンド開始・supervisor の更新キー観測を順に確認する。短い実時間枠内での起動速度や旧 watcher を期待せず、期限境界は純粋な LeaseClock 試験で検証する。観測 journal は best-effort 診断であり、lease の判定やコマンドの継続条件にしない。

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

S15f の最初の slice は読み取り専用 `estimate` とする。既に起動している認証済み owner にだけ照会し、停止時は unknown を返す。command forecast は `forecast_command_v1` の交渉が必要で、既存 canonical clone と実際の profile fingerprint から TTC の正規化 command history を参照する。clone・workspace・source・attempt は作成せず、履歴がない場合の fallback は順位付け専用で ETA として表示しない。source の repository identity が判明しない場合や clone が存在しない場合は、別の cwd の履歴で代用しない。

nested cwd は標準実行の cwd 合成との一致を別途修正・検証するまで履歴を unknown とする。既存 clone でも TTC の main-worktree identity を検証できない場合は履歴を流用しない。

標準 supervisor が TTC の verified execution outcome を履歴へ記録していなかった欠落も接続する。実登録 ledger の key・weight と実測 wall/CPU/RSS を、停止・finish が確認された自然終了（不成功 exit code も保持）だけで idempotent に記録し、取消・timeout・追跡不能・停止未確認は学習しない。履歴書込失敗は計算結果を失敗へ変えず、finish 後の crash は一標本を失うことを許容する。

この slice で S15f は完了しない。転送・setup・成果物回収は実測モデル未接続のため unknown と明示する。source-ready・setup receipt・collection-complete の検証済み境界で、identity・TTL・sample count・provenance を保つ bounded な観測を記録し、実配置と estimate が同じ CostModel を使う接続を必須残件として維持する。queue wait を転送時間へ混ぜず、結果の総 byte 数が不明なら回収コストも unknown のままにする。

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

- ユーザーの受入環境では追加の Mac/Linux 実機を準備しない。当面の追加 worker と Linux 検証には承認済みの Docker を使う。別 state dir と portool で分離し、複数 worker、親の再起動、接続断、ACK 消失、worker 消失、cold/warm と親単独との差を確認する。
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
now uses independent owners and the shared source/secret delivery helpers for
explicitly registered private user-config workers. Real SSH/Docker and native
acceptance remain required; the fake-SSH process fixture is a focused regression,
not completion of provisioning or native lifecycle acceptance.


### Resident source-ready gate slice

`SubmitCaptured` の `Accepted` は run/task が durable queue に記録された意味とし、worker の source 受領や `WorkerAccepted` receipt とは区別する。local resident も source IO を独立した bounded background job へ移し、実際の worker `BlobVerify.ready` 後だけ new placement を許可する。接続ごとの volatile cache は最大 64 run とし、run/source/manifest digest と connection generation が一致した forecast だけを commit に使う。queued/live の metadata scan と secret delivery は keyset page、eligible run/worker union は SQLite `json_each` を使う。未評価 cache entry は保持し、dispatch 評価済み entry は FIFO eviction で後続 run へ進む。gated dispatch も keyset cursor で同 run 内の大きな backlog を走査し、filtered page で既存 waiting state を消さない。

source export/upload の共有 transfer lease と cleanup は background job 間で維持する。admission は network や transfer mutex を待たない。停止後の残 IO は capture store と lease だけを保持し、旧 coordinator owner を保持しない。secrets は既存 attempt/generation へ配置後に送り、worker の Preparing/lease gate を維持する。local に事前配置した legacy source も再起動後に worker verification で Ready を再構築する。

接続 owner の追加 slice は private user config の既存 `[[workers]]` を起動時に snapshot し、`id` を期待 worker identity、`ssh` を接続先として独立した serial lifecycle で維持する。hello の cluster/name/protocol 検証後だけ source gate 付き link を attach する。切断は gate 無効化、接続を照合した detach/unknown、所有 SSH child の reap、stop-aware bounded backoff の順とし、同じ worker/boot の再接続でも delivery cache を新しくする。登録の再読込・自動登録は行わない。worker add/install・鍵登録・native service・upgrade/rollback・ephemeral と実 SSH/Docker/native acceptance は必須残件であり、S15b 以降と S22 の完了を意味しない。schema migration は不要。

### S15 の利用者経路の統合

S15a（PR #40、merge `8939ef2`）と S15b（PR #41、merge `05e981c`）は main に統合済みで、各 HEAD のローカル実行 9 ジョブ・リモート 9 ジョブが成功した。S15c の PR #42 はローカル実行 9 ジョブ成功後、Linux の失敗子プロセス fixture が startup lock の試験へ到達しない問題を検出した。専用の private executable と起動 marker へ置き換え、同じ lock・中断・再起動の検査を保って再検証中である。

managed init、独立 worker role、配置前の source readiness、双方向に revision で fence する Cancel/Retry を、利用者の local 実行経路の区切りとして統合する。異なる設計の担当コミットと独立レビュー結果を保持し、統合時の旧 IPC fixture と非同期 adoption の観測 race は別のテスト修正で扱う。storage 86 件、resident unit 8 件、completion 4 件、coordinator 41 件、retry/secret の実 CLI、resident integration 6 件、worker role 6 件と strict clippy が統合後に成功した。転送完了を待たず durable queued を返し、source gate と接続交換は SQLite commit 完了まで同期する。全体 CI とリモート検証が終わるまで本流へ統合しない。

`--in-place`、requires/profile、条件付き・glob・複数 path の出力計画、公開 logs/outputs、結果 retention と reader pin、SSH の接続所有・再接続は実装と個別検証後、同じ S15 feature branch へ統合した。統合後の全体 CI・公開は未完了。source/Vault の保持・消去、読み取り専用 estimate と実測 cost 接続は必須残件として継続する。

統合 HEAD の全体ローカル CI は通常テスト・coverage を通過した一方、mutation の unmutated baseline で init readiness 2 件と captured submit 1 件が期限切れになった。原因は未確定で、変異の結果とは区別する。private fixture の owned child にだけ Tokio worker 数を制限し、init の終了状態・private stderr の固定分類、submit の bounded TTC ledger/sidecar metadata と worker journal を期限切れ時に残す。本番の CPU guard・期限・登録手順は変更しない。対象 3 件は各 3 回と strict clippy が成功した。診断追加後の全体 CI とリモート検証は未実施である。

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

Generic outputs worker/collector slice adds frozen declarations, success/failure/always
eligibility and TarV1 opaque bundles. Multiple globs and literal directories preserve
workspace-relative member paths. Additive schema-1 serialization retains historical raw
canonical bytes; every live hello requires output_bundles_v1. The journal fixes encoding, outputs_evaluated
and all eligible stored/not-stored names before terminal settlement; collector exact-name
receipts and ACK remain opaque. Public outputs query/export and resident result GC are integrated below; estimate and
CI intermediate upload/download integration remain required follow-up work.


Public outputs slice connects paged read-only immutable artifact metadata and durable receipts to legacy raw and TarV1 export. Private bounded spooling verifies outer size/digest; full tar structural/path preflight precedes fd-based no-replace publication. It preserves partial successes, emits bounded NDJSON outcomes/final counts and reuses logs' interruptible final emitter. Real worker execution through immutable bundle, parent receipt and offline public CLI export covers raw plus two generic members, no Git, unchanged epoch and historical-attempt conflicts. Focused hostile archive, symlink, concurrent no-replace, receipt visibility, metadata paging and retry boundary tests accompany the change. Result GC/pinning is integrated below; estimate and CI intermediate artifacts remain mandatory follow-up work.

Public outputs independent review corrections restrict artifact query records to name, encoding, digest and size; declaration paths and execution identities remain private. Producer and retained export share the reserved component namespace and existing Unicode/case folding. Retained archive validation failures use Internal (79), while destination argument errors and no-replace conflicts retain their existing codes. Regression coverage includes exact public metadata keys, reserved nested/alias paths, malformed archives with matching outer receipts, and zero member publication before complete preflight. GC invocation pin integration is completed in the integration section below.
Focused output tests (13), real offline CLI corruption/export regressions (2), and strict all-target clippy pass for these corrections. Independent resolution review and combined full CI remain parent integration checks.

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
pin 準備失敗時に run/task が公開されない回帰と、実 resident IPC の submit を全 worker offline で受理した直後に logs 共通 observer が pending を返し follow を継続する回帰が成功した。追加修正後の strict clippy all-targets/all-features も成功。独立レビューで元の3件と enqueue pin の解消を確認済み。統合後の全 CI は未実施。

### Public outputs と result retention GC の統合

独立確認済みの public outputs と result retention/in-place feature を統合した。
TaskSpec/AttemptPayload の outputs と in_place、明示 encoding/outputs_evaluated、schema 8 の
result refs/clock/pins は同時に保持し、legacy raw serialization と両 PURE 対象・coverage union を残す。
outputs は configured read-only store の identity 検証と run shared pin 取得後に fresh metadata を読む。
pin は処理ブロック外側で保持し、owned blocking export worker の cooperative cancel/drain と
最終 write/flush（backpressure・SIGINT を含む）の完了後に解放する。logs Observer の既存 pin も保持する。
metadata query は fresh durable result/blob refs と既知の delete claim を確認し、expired/unreferenced/claimed は
NOT_FOUND / 73 とする。claim 永続化後・FS 削除前の owner crash でも残存 file を成功 export しない。
live ref の published file 欠損・短縮・破損は INTERNAL / 79 を維持し、全 NotFound の一律変換を行わない。
reader は owner 起動・Git/project 読取・DB 更新・pin 作成を行わない。

この統合は result store のみを対象とする。source/Vault/private in-place binding の GC は引き続き必須残件で、
原本 filesystem は削除対象にしない。全 CI・公開前検証と最終 acceptance は親統合で行う。
Docker/Linux の承認済み受入と、追加 Mac 実機の sleep/boot/実 launchd 未確認は上記受入環境に従い区別する。

統合後の focused 検証は outputs/export unit 15、logs unit 11、public outputs CLI 3、guarded retention 4、
indexed run 選択 1、migration 2、legacy hash/encoding 2、実 resident GC 競合 1、in-place integration 9、
通常 snapshot/offline control 1、collection integration 39、実 worker から offline raw/Tar export 1 の計 89 件が成功。
raw/Tar の export outcome・final write・final flush をそれぞれ block して GC 排他拒否を確認し、
実 CLI の Tar worker 中断は cooperative drain・temp cleanup 後に pin を解放する。
実 CLI で live-ref file 欠損 79、owner restart/claim-before-FS と削除完了後の 73 を区別した。
strict all-target/all-feature clippy、fmt、diff check、公開禁止文字列 scan は成功。
今回新しい pure decision helper は追加しておらず、元 feature の PURE/coverage union と clock/mutation 証跡を保持する。
全体 local CI・MSRV・Linux と公開はこの focused 検証に含めない。

### S15 worker capabilities の接続

worker-local `profiles.toml` の active profile、実際の PATH での固定 tool probe、durable な execution requirements、forecast 前の候補除外と worker の独立 admission を接続する。project/CLI profile は配置 selector であり worker の環境を変更しない。profile fingerprint は setup/template、artifact と TTC command history に使い、受理時の実 identity は worker 内の記録へ固定する。tool drift は report を無効にし、再検査に通るまで新規受理・準備を拒否する。旧 peer は制約なし task のみ互換とする。

この接続では S20 の version-file readers・baseline・typed match・nodenv/rustup/mise/fvm/corepack の detect/resolve/install/activate、S21 の diff/sync/doctor、S22/S23 の実機 service 環境 acceptance は完了しない。profile の nonempty `secret_refs` は安全な provider が未接続のため明示 unsupported とする。adapter/installable/secret provider を成功として申告しない。Flutter 標準 launcher と Dart bootstrap wrapper は `--version` でも SDK を導入し得るため、この slice では Unknown とし、S20 の noninstalling snapshot/native probe を必須残件とする（native Dart は測定する）。optional tool は固定された最大 14 fixed calls を各 300ms 以内で probe する。要求された tool のみの lazy probe/cache、installed-version inventory と private tool-root adapters は後続の必須実装へ引き継ぐ。


### S15 機能接続の確認（2026-11-14）

SSH owner、公開 logs/outputs、in-place、結果保持、private profile と能力判定を同じ feature branch へ接続した。
outputs と requirements は両方を payload・計画 digest へ保持し、両方の feature 交渉と PURE/coverage/mutation 対象を統合した。
TTC history は profile だけの上書きにせず、正規化された command identity と実 profile fingerprint を組み合わせる。

接続検証で local-only 起動に SSH 登録専用の private-file 検査を適用する問題と、非 Git directory の project 設定を読まない問題を修正した。
remote が設定されている場合の private 登録検査は維持し、directory の source identity は非 Git のまま出力・除外方針を読む。
関連 51 件、全 target/all feature の型検査、strict clippy と fmt が成功した。
実 CLI で profile/requires と required/always output を同時に使い、元 snapshot の retry でも制約・出力宣言・回収済み artifact を保持することを確認した。
SSH の2 worker・切断・再接続・local 継続は2回成功した。途中の期限切れは原因未確定として扱い、私有 child の runtime thread 数制限と bounded 起動/attempt 診断を fixture に追加した。
worker spec の env 許可制は維持し、runtime 制限は所有プロセス側で行う。
この統合 HEAD の全体 CI・リモート CI は未実施であり、S15 完了とは扱わない。

PR #43（merge `98b91156ff22e0a6e72a41b0d084a8f113dc3584`）は lease seed の単一 monotonic 計測と owner Drop 時の明示 unlock を統合した。
対象 `9b725b7` のローカル9ジョブ、リモート9ジョブが成功した。ローカル未対応 installer と Docker cleanup 未確認は成功へ置き換えていない。
PR #42 の修正へ接続した `de35f94` はローカル7ジョブ成功、2ジョブ失敗。mutation shard 3 の unmutated baseline で setup 4件・workspace hook 1件・captured submit 1件が失敗し、残る1件は report 不足による gate 失敗である。
通常テストと coverage は成功しており、baseline の原因確認・修正を継続する。変異 survivor や正常終了として報告しない。

source 保持は別 feature で削除しない root/index・cross-process guard・候補 preview を先行実装中で、実消去は未有効。
S15f、S16、S18〜S26 の必須範囲と Docker 受入・実機固有の未検証範囲は維持する。

### PR #42 の baseline fixture 補正

lease/lock 修正を接続した全体検証では通常テスト・coverage と mutation 3 shard が成功したが、
残る shard の unmutated baseline で setup 4件・hook 1件・captured submit 1件が失敗した。
setup の目的は共有・再構築・停止であり、lease expiry の試験ではないため、
全 fixture に与えていた人工的な3秒 leaseを外し production default を使う。
1秒の step timeout、本番 lease・更新間隔・CPU guard は変更しない。
hook の descendant 試験は実 shell と child の起動予算を10秒とし、実際の timeout・stderr・child停止の検査と30秒上限は維持する。
captured submit は fixture が所有する daemon の Tokio thread 数だけを制限し、
失敗時の bounded TTC/worker metadata を残す。実行中との推測に Ready を使わない。

fixture 補正 `72b3f09` で setup 全30件、hook 1件、captured submit 1件、
strict all-target/all-feature clippy と fmt が成功した。
以前の interrupted が lease expiry だったことは追跡されておらず、起動・スケジュール感度という推定と区別する。
補正後 HEAD の全体 CI とリモート確認が終わるまで PR #42 を統合しない。

S15 の追加 fixture 点検で、macOS 専用の temp path を34か所検出した。
canonical な /tmp 配下へ統一し、Macでは従来と同じ private path、Linuxでは存在する temp root を使う。
出力15件・macOS backend の fake/観測27件・init の private/fake 15件が成功し、fmt も成功した。
実 Linux での確認は統合後のリモート CI で行い、実 launchd の受入としては扱わない。
### Source retention の非削除 foundation

source store 専用の永続 mutation lock と retention index を追加する。capture 前の unknown pending root、
immutable manifest/submodule/blob graph の 32 参照ずつの永続登録、durable run root への一致 handoff、
未 submit capture と crash root の保守保持を扱う。旧 manifest は bounded page で保護 root へ登録する。
同じ store の shared guard を capture/import、submit/enqueue、retry commit、source/secret delivery と
blocking transfer drain まで保持し、source lock は admission/actor/DB transaction より前に取得する。
index は coordinator DB と別の所有領域で、cross-DB atomicity を保証したとは扱わない。

この段階は dry-run のみで、root 失効・利用不能 tombstone・不可逆 deletion claim・source/Vault の
実削除や resident automatic source GC は追加しない。trusted boot/monotonic cutoff、同一 store/source/run
binding、全 generation の known terminal と collection closed の正の証拠から、最大 32 件の owned
manifest/blob 候補を示す。未知・不正・query error・pending・shared digest は保持し、collection failure を
成功へ変えない。source/Git refs/bundles と Vault の実削除、旧 unsubmitted capture の安全な reconciliation は
引き続き必須残件。private in-place binding と原本 filesystem はこの cleanup 対象に含めない。

非削除 foundation の focused 検証は新規 root/preview 9 件（owned helper は単独 subprocess として実行）、
retry guard 1、retry 停止と input IO drain 1、source capture 38、実 worker/offline outputs 1、in-place 9、
既存 same-run retry 11 の計 70 件が成功した。shared digest/submodule、capture-before-bind fault、
unknown root 保持、clock Hold の cursor 保持、foreign coordinator/source/guard、manifest identity、
source root/ancestor symlink と canonical path alias を確認した。strict all-target/all-feature clippy、
fmt/diff と公開文字列確認が成功。全体 CI/MSRV/Linux はこの focused 検証に含めず、実削除は未実装のままとする。

source index のレビュー修正として、32 参照ごとの full manifest 再読込・再検証を除去した。
foreground capture/receive は既に保持する manifest を登録し、run root は検証済み membership を
32 件ずつ引き継ぐ。background は検証済み manifest 1 個と FD identity を cursor に保持し、
ページごとは identity のみ検査する。初回読取は receiver 既定と同じ 64 MiB 上限で、超過は
`Preview.manifest_limit` の型付き recoverable Hold とする。`PreviewCursor.manifest_byte_limit` を
明示的に上げると既存の大きい manifest も登録でき、通常の source 利用や foreground capture を失効させない。
自動で任意サイズの旧 manifest を扱う streaming staging は後続事項とし、全 legacy GC 完了とは扱わない。
130 参照を跨ぐ読取/検証 1 回、cursor 再起動時だけ再読取 1 回と永続 position 再開、foreground の
再読込 0 回・検証 1 回、上限 Hold から明示 bound 変更での再開を回帰で確認する。
received root を将来失効させる前には、hash 共通の pending blob 保護を per-import token/fence に分ける必要がある。
現在の received root は永久保守保持で、今回も actual source/Vault deletion は追加しない。

submodule の発見による cache eviction も同じ修正で防ぐ。current manifest の graph 行を完了するまで
優先し、新しく発見した child の ID が parent より前に sort しても切り替えない。
131 entry の parent と先に sort する 65 child の回帰で、foreground は held parent の再読込 0 回、
background は各 66 manifest の読取/検証がそれぞれ 1 回だけであることを確認した。

修正後は root/preview 13、retry guard 1、retry 停止/drain 1、source capture 38、実 worker/offline outputs 1、
in-place 9、same-run retry 11 の計 74 件と strict all-target/all-feature clippy が成功した。
submodule の current-manifest 優先修正後にも source capture 全 38 件を再実行し成功。
親レビューは flat と submodule-preemption の P2 を解消確認済みで、他の指摘はない。
全体 CI/push と actual source/Vault erasure は今回の非削除 foundation に含めない。
S20a adds captured version declaration readers and typed scoped requirement /
immutable baseline APIs. Remaining S20 work: adapter detect/resolve/install/activate,
private roots and singleflight, frozen submit/admission integration. S21 diff/sync/
doctor and S22/S23 service acceptance remain required; S20a alone is not S20 completion.

### S15/S20 API の統合確認（2026-11-15）

読み取り専用 command estimate、検証済み実行時間の TTC history 登録、
source 保持の非削除 roots/guards/index、S20 の宣言・凍結契約と5種の private adapter APIを
同じ feature branch へ統合した。outputs・capability・toolchain の coverage/mutation 対象は
和集合を保ち、schema v9 の fence と source store 専用 index の所有を分けた。

通常 snapshot の command cwd は attempt root 相対として解決する。
nested Git project からの公開 exec と in-place の実行を確認し、元入力と親ディレクトリへの
アクセス、実際の回収ログ、snapshot が元ファイルへ書かないことを検証した。
estimate は実 worker の2回の実行から得た履歴を読み、未知の transfer/setup/collection は
未知のまま表示する。実測 cost の永続化と本配置との共通 model 接続は引き続き必須である。
履歴の統合 fixture は実 fast-worktree とシステムコマンドだけを PATH に置き、
無関係な optional manager の起動遅延へ依存しない。通常 profile の probe 最適化は別の残件である。

private adapter の独立レビューで Go の自動 compiler 切替と probe argv を修正し、
probe/consumer に GOTOOLCHAIN=local、probe に go version を固定した。
source ごとの契約 binding と実行環境 fingerprint は分離し、consumer の HOME は維持する。
実 installer を Setups/supervisor/TTC の同一 lifecycle へ接続する S20c、
TaskSpec/submit/worker の凍結・activation 接続、S21 の diff/sync/doctor はまだ完成していない。
adapter API だけを理由に自動導入可能とは申告しない。

PR #42 の 21be1ec はローカル実行9ジョブ成功、リモート通常テストの Linux/macOS と
MSRV 1.89 が成功した。coverage と各 mutation baseline は captured-submit の command 開始前に
失敗したため、通常テストだけにあった fast-worktree 導入を両ジョブへ追加し、
terminal failure でも bounded diagnostic と owner drain を実行する。補正後 HEAD を再検証中で、
リモートの全チェック確認前には統合しない。
source/Vault の実削除、S16・S18〜S26 の必須実装、Docker 受入・公開は残件として維持する。

統合後の focused 検証は toolchain 42件、source retention 13件、readonly estimate の
DB全行不変1件、retry source guard 1件、公開 estimate 2件、nested exec 1件、
captured submit 1件、Forecast 7件、source capture 38件が成功した。
all-target/all-feature 型検査、strict clippy と fmt も成功。
captured submit の retry は一度 exit 1 になり、追加した安全な診断付きの再実行では成功した。
この失敗の原因は未確定であり、全体 CI で再発した場合は診断に基づき解消する。
通常 profile の eager probe による遅延や一時的な観測差は調査対象として残し、
focused 成功だけで全体の安定性確認を完了したとは扱わない。
### S20c private preparation infrastructure（2026-11-15）

trusted な concrete adapter request を受ける内部 `Setups::poll_tool` を追加した。
5種の adapter は既存 builder の singleflight、detached supervisor、TTC permit、
lease/cancel/drain/recovery を共有し、private root へ導入する。helper は private request と
spec の binding、worker acceptance/launch identity、live managed Running command の
正確な child identity を確認するまで manager を実行しない。source contract と artifact key、
profile definition と実 execution fingerprint はそれぞれ分離する。

完了 receipt は現在の外部 identity と tree を再検証する。最後の consumer が離れた後も、
旧 writer の settlement が不明なら同じ key の新規導入を止める。partial/drift は blocking failure
とし、tool root の自動削除・再導入や workspace template への置換を行わない。

private fake manager と所有する supervisor/TTC process の9件、private file 検査1件、
既存 workspace setup の31件が成功した。strict clippy all-targets/all-features と fmt も成功。
実ホスト manager・network・service 導入は実行していない。全 CI はこの差分では未実施。
TaskSpec/submit/worker routing と consumer activation、inventory/diff/sync は未接続であり、
この infrastructure だけでは S20 全体の完了や公開 task の自動導入を申告しない。
private preparation の review 補正では、receipt/tree の検証と detect を共有 builder mutex の
外へ移した。検証後に consumer の世代・binding・flight・settlement barrier・同じ key の
writer 世代を再確認し、release 後の復活や古い Missing 判定による二重導入を防ぐ。
所有確認と finish も mutex を同時保持しない。検証を channel gate で止めたまま、
既存 lease loop と共通の tick が無関係な workspace/tool builder の renewal と cancel を
WorkerStore へ永続化し、release/finish/所有確認まで進む回帰を追加した。
補正後は causal/recheck unit 3件、private process 9件、既存 setup 31件、strict clippy と fmt が成功した。

preparation の profile identity は、親が凍結した capture provenance と worker-local な
operator definition を分離した。`PreparationRequest.profile_definition` は信頼済みの
worker 設定から与えて照合し、`base_profile` は引き続き worker の実 execution identity とする。
`frozen.profile_definition` と resolution authority は親の捕捉契約のまま変更しない。
local definition は helper の request binding に含めるが、再利用 artifact key へ追加しない。
wire/submit からの worker request 構築と consumer activation は引き続き未接続である。
異なる親/local definition の private fake-manager install/reuse と、誤った local definition の
実行前拒否を含む process 10件、focused unit 4件、strict clippy と fmt が成功した。

S20c の worker-local registry infrastructure は、private `toolchains.toml` の
schema 1、閉じた5 adapter recipe、bounded explicit inventory、active local profile binding
から `PreparationRequest` を構築する。既存の parent resolution evidence を検証・保持する経路と、
local inventory authority で未解決契約を解決する経路を分けた。local profile definition と
既存の実測 execution fingerprint は信頼済み worker factory から与え、parent provenance を
置換しない。resident 中は immutable snapshot とし、設定更新には drain/restart を要する。
request/detect は process・network・mkdir・manager listing を実行せず、exact plan receipt のみ
検証する。inventory は導入済み版の申告ではない。TaskSpec/submit/resident routing、consumer
activation、installed inventory index、diff/sync、forecast は残件であり、自動導入 capability を
公開しない。API と信頼境界は [private adapter API](toolchain-adapters.md) を参照する。
registry unit 9件、private preparation process 11件（新規 registry→Setups bridge を含む）、
strict clippy all-targets/all-features と fmt が成功した。検証は private fake-manager のみで、
実ホスト installer・network・Docker/service は実行していない。full CI はこの差分では未実施。
複数 tool は全選択を確定後、original evidence を保持して同じ complete frozen contract から
全 request を再構築してから導入する。baseline-only は `BaselineCheckRequired` とし、
worker routing での実測 base observation と frozen policy の照合を残す。


### 公開 toolchain 接続と検証の区切り（2026-11-15）

公開 submit から source-bound frozen contract、read-only resolution、durable acceptance、
既存 Setups/supervisor/TTC、consumer activation と recovery へ接続した。
base profile と attempt の effective fingerprint を分離し、受入後の有効 receipt 置換も
完全な activation binding の照合で起動前に拒否する。稼働中 process の adoption は維持する。
独立レビューの P2 は解消確認済み。private fake-manager を使った公開 CLI の
並行版・retry・setup/consumer・復旧回帰と focused units、strict clippy、fmt は成功した。
実 manager の導入、変更後の pure coverage/mutation と全 CI は未実施であり必須残件とする。

検証基盤は local-ci 0.12.1 のローカル10項目とリモート10項目が成功し、PR45をマージ、
crate とリリースタグを公開し、4環境の配布物とchecksum計8件を照合した。
公開crateも検証済みソースと一致し、手元のCLIを0.12.1へ更新した。
RTR PR44 の timing fixture 補正はローカル9項目・リモート9項目成功後にマージした。
PR42 はその補正と CI 依存準備を取り込んだ committed HEAD の全体検証を実行中。
ローカル未対応 installer・Docker 環境は未検証として区別する。

追加実機は未準備との回答に従い、S26 は Docker の複数 worker/Linux で実行可能な
受入を進める。Mac の実 sleep/boot/launchd と追加 Mac 固有の確認は未検証として残す。
S16 の SDK 修正と実行 driver、実測 transfer/collection/setup、S18〜S26 は必須残件。
source/Vault 実削除の具体案は回答待ちで、Docker 利用の回答を削除承認とは扱わない。


### 実測モデルと保持ガードの統合（2026-11-15）

PR42は5a990d5のローカル9項目とリモート9項目成功後、2b3078cでマージした。
source保持ガードの初回並行作成は、このMacのnative openatで競合時ENOENTを再現し、
atomic create-newと既存openへ分離して解消した。retention DBの検査用FDが稼働中
SQLiteの排他を解除する問題も、別processによる旧方式失敗・修正後成功で確認した。
既存DBはmetadata検査、新規DBはprivate tempを閉じてからNOREPLACE公開とし、
private mode・nofollow・nlink・root identity・永久SH/EX lockの条件を維持した。
保持unit18件、並行投入10回、strict checkと独立レビューが成功。実GCは追加していない。

転送は両lease取得後から現在generationのsource-ready確認まで、回収は検証済みterminal tailの
active IOだけを記録する。durable close、boot/epoch・exact入力・TTL・bounded dedupと
永続化後のメモリ参照を使い、配置とestimateが同じCostModelを読む。writer障害は計算結果を
変更しない。確認済みreadyの転送0は標本数0の別の証拠として表示する。
known cold候補は期限付きprefetchだけを選び、source readyになる前にattemptを作らない。
frozen toolchain適合はwarm/cold双方に独立した必須条件とし、readonly照合はreadyを先に行う。
実private RPCの128KiB転送・計算・回収・100回preview、metrics書込失敗時の計算継続、
source gate、cold選択・適合拒否、strict checkと独立レビューは成功した。

pure contractとprefetchをcoverage/mutation gateへ追加した。統合HEADの全CIと
100% line coverage・zero-survivorは次の必須検証で、まだ成功とは申告しない。
setupの実測producerと公開の既知task estimate入口は残件。prospective commandの
exact source/outputが不明なコストはUnknownを維持する。大きいmanifest等の完全転送を
証明できない場合もUnknownで、配信自体の成功を妨げない。

S16 SDK 0.13.0候補はローカル10項目が成功しPR46でリモート確認中。
shared driverと既存Runner評価の共有、dynamic matrix・artifact交換・shard retry・
親の厳密status判定、S21〜S26とDocker受入は必須範囲として継続する。


### 統合検証のfixture補正とreadonly入口（2026-11-15）

統合c30c0b6の全CIは通常試験2件が失敗し、coverageのGit FIFO fixtureが停止したため
所有する検証だけを取消・回収した。coverageとmutationを成功扱いにはしていない。
停止は複数Git probeへ一度しか解放しないfixtureが原因で、Machine preflightだけを
対象にboundedに解放・回収するよう修正した。同期preflightへの制御変更では
期待のassertで失敗し、通常とinstrumentedの関連起動試験で成功を確認した。
旧schema再現に残る新index/markerと、実追記前にshutdownへ進めるfixtureも補正した。
productionのlease・timeout・drain条件は変更せず、関連strict checkも成功した。

`estimate --task ID` は保存済みspec/sourceを使う追加実行のreadonly予測として接続した。
raw commandの既存形式を維持し、全commands/env/requirements/toolchainsと同じcost bindingを
使う。current projectやGitの再探索・owner開始は行わない。raw100回＋known100回の
DB/epoch/actor/gate/source/ledger不変と、変更後workspace・offline/不正IDを確認した。
独立レビューも成功。native runtime履歴を識別できない既知taskはUnknown/fallbackを保つ。

準備観測のproducerは、確認済みTTC admitted executionだけをimmutable private proofへ
記録し、全node成功とtemplate publish/tool detect後のcurrent builderに結び付けた。
policyはsum-ttc-admitted-run-wall-v1でspawn overheadを含み、queueや待ち区間を含めない。
worker owner再起動時は古い観測を再利用しない。関連97試験と独立レビューが成功した。
worker wire・parent ingestion・remote age/TTL変換・pagination・warm proofと、forecastでの
同一builder二重計上除外は次の必須接続で、setup costは引き続きUnknownである。

local-ci0.13.0はローカル10項目・リモート10項目後に公開済み。
次は統合HEADのpure coverage/mutationを先に絞って確認し、全CIを再実行する。

新規pure 2モジュールの限定検証では、contractのbaseline不明・alias未証明・
最大256 evidenceの試験を補った後、contract411/411・prefetch215/215行がcoverage100%。
contract35・prefetch46のmutantをcaught、survived0・timeout0・unviable11だった。
prefetchは同一blobの既存結果を再利用した。これは限定結果であり、統合全CIは次に確認する。


### 統合CIの再検証で見つかった問題（2026-11-15）

`98945fc` の全体ローカルCIは2成功・4失敗・2取消・1skip、50分42秒で終了した。通常UbuntuとMSRV名のジョブは成功したが、ローカルRustは1.100であり1.89確認ではない。通常macOS名のbootstrapはEnvironmentMismatchによる未受理、coverageはcapability390/391行・wire1709/1710行、mutation2/3は回収fixtureの64件期待に対し63/60件でbaseline失敗した。mutation0/1は取消で、完全な変異検出結果は無い。

mutation routing traceではGit起動がfixture開始7秒後となり、5秒でready FIFO読取側が閉じていた。所有FIFOへの一時readerで自然解放したが、その実行を成功gateに使わない。ready/release両FDをstartup完了まで保持する修正を統合した。初期Git到達待ちはbootstrapの20秒契約と合わせ、executor応答の5秒watchdogは維持した。6秒遅延Gitが成功し、同期startupへ一時的に戻す退行は5.57秒で検出、最終fixtureも成功して製品sourceは復元済み。残るprofile観測・回収fixture・pure不足を修正してから、コミット済み統合HEADの全CIを再実行する。


989で失敗した3経路は修正統合`208987d`で、bootstrap・起動preflight・部分ページ回収をnextest jobs2へ絞って再実行し3件とも成功した。capabilityのinstallable bundleがprofile/OS/arch/feature条件を免除しないこととbundle無しの拒否を追加し、wire test helperの不要な型分岐を共有response値へ整理した。限定46件成功、coverageはcapability411/411行・wire1709/1709行、mutation130件はcaught117・survived0・timeout0・unviable13。profile修正は起動時Knownから一時Unknownへの変化だけを許容し、実体/版の変更と初期UnknownからKnownは従来通り再attestationを要求する。必須ツール・固定baselineはlive Unknownで拒否し、独立安全レビューでP1/P2無し。実際のCI不一致の方向は未確定なので、全体CIで再確認する。

### Source/Vault deletion A: 非削除の ownership / availability fences

ユーザーは具体的な保持・削除条件に対して「この条件で実装・有効化する（推奨）」と明示承認した。
承認は後続の実装・有効化まで持続する。今回は A の foundation のみで、実際の root expiry、
削除 claim の作成、unlink、Vault erasure、resident 自動削除の有効化は追加しない。
B/C と削除専任レビューを経て実削除へ進む。ユーザー実 state は検証に使わない。

取り込みごとの durable token と store identity、publishing / published / bound の状態・正確な run 予約を追加し、
同じ hash を受け取る別 import の保護を解除しない。handoff と membership release は一度に32件以下の
transaction とし、crash / 不一致時は保護を残す。token 無しの旧 API は独立した保護 root を残し、
hash 共通 marker の一括解除を廃止する。旧 unknown / legacy / unsubmitted root は保持を続ける。

将来の run expiry fence と source/object publication incarnation を別の型・source-owned table で表す。
content-addressed SourceID の生成式・同一入力IDは変えない。B の claim 完了後、同じ ID/hash の
新 publication incarnation は可能だが、旧 run の失効は解除しない。A は production から expiry / claim
状態へ遷移させない。将来の Vault-only SecretUnavailable fence は別の責務で、source bytes の7日保持や
共有 source の読み取りと混同しない。

publication / bind / enqueue / retry preflight+commit / source read・transfer・readiness に将来の fence 検査を接続する。
source-first / EX nonblocking を維持し、actor の中で source lock を待たない。公開 object_store は guard-owned
view とし、SDK raw handle を通常の公開 API から返さない。crate 内 SDK 呼び出し用の with_sdk callback は
全 callback 中 SH を保持する。SDK は書きながら hash を発見するため、pending publication claim 中は raw SDK
操作を保守的に拒否する。S16 の build_frozen_plan_from_source はこの callback 内へ適応する。
UploadFile の reader 自身も SH を所有し、async waiter の取消後も実 blocking read / upload drain まで保持する。
信頼 caller の意図的な filesystem / SDK handle 持ち出しまで防げるとは主張しない。

private fixture の source retention 26件、retry 関連25件、toolchain 63件、source capture 38件、worker blobs 34件、
forward upload 1件と strict all-target/all-feature clippy、fmt/diff check が成功した（群間に重複あり）。
source subprocess helper 2件は standalone ignored で、親テストが必要時に直接起動する。
全体 CI / push / 実削除の検証は A の対象外とする。
