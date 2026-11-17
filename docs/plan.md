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
| S15c | `init`・source/secret capture と durable `submit`・`ls`・`show`、run/task の問い合わせ API | 同時 CLI でも coordinator epoch が増殖しない。capture 失敗時は配置せず、完了済み run も正しく列挙する。JSON と stderr の契約を検証 | S15b、起動方式は S22 と整合。本体は PR #42（merge `2b3078c`）と PR #45 で main に統合済み。残件 5 項目は実装済みで、根拠は下の「S15c 残件の確認」に記す。全体 local-ci とリモート CI は親の gate |
| S15d | `exec`・`wait`・`cancel`・Ctrl-C・明示 `retry`・local-only `--in-place` | exit code を透過し、停止不明を cancelled にしない。unknown の retry を拒否し、元 snapshot と履歴を保つ。in-place を remote に配置しない | S15c。実装とテストは main にあり、根拠は下の「S15d・S15e の確認」に記す。統合 HEAD の全体 local-ci・リモート CI・remote worker での検証は親の gate で、未了 |
| S15e | `logs --follow`・再接続・`outputs`・retention の常駐 loop への接続 | 非 UTF-8・offset・切断再開、出力 path 衝突と部分失敗、回収失敗、実行中・回収中・保持対象を消さない GC を検証。自動削除に件数上限と時計跳躍の保護を付ける | S15d・S14。接続は main にあり、Vault の公開途中の停止からの回復と不足していた検証を足した。根拠と未了は下の「S15d・S15e の確認」に記す |
| S15f | 読み取り専用 `estimate`、配置理由、転送・setup・回収の実測値の接続 | estimate が queue・attempt・ledger 需要を増やさない。measured/fallback/unknown・TTL・priority の説明を保つ。S15 の受入条件を利用者経路で閉じる | S15b〜S15e、S11・S13 |

S15f の最初の slice は読み取り専用 `estimate` とする。既に起動している認証済み owner にだけ照会し、停止時は unknown を返す。command forecast は `forecast_command_v1` の交渉が必要で、既存 canonical clone と実際の profile fingerprint から TTC の正規化 command history を参照する。clone・workspace・source・attempt は作成せず、履歴がない場合の fallback は順位付け専用で ETA として表示しない。source の repository identity が判明しない場合や clone が存在しない場合は、別の cwd の履歴で代用しない。

nested cwd は標準実行の cwd 合成との一致を別途修正・検証するまで履歴を unknown とする。既存 clone でも TTC の main-worktree identity を検証できない場合は履歴を流用しない。

標準 supervisor が TTC の verified execution outcome を履歴へ記録していなかった欠落も接続する。実登録 ledger の key・weight と実測 wall/CPU/RSS を、停止・finish が確認された自然終了（不成功 exit code も保持）だけで idempotent に記録し、取消・timeout・追跡不能・停止未確認は学習しない。履歴書込失敗は計算結果を失敗へ変えず、finish 後の crash は一標本を失うことを許容する。

この slice で S15f は完了しない。転送・setup・成果物回収は実測モデル未接続のため unknown と明示する。source-ready・setup receipt・collection-complete の検証済み境界で、identity・TTL・sample count・provenance を保つ bounded な観測を記録し、実配置と estimate が同じ CostModel を使う接続を必須残件として維持する。queue wait を転送時間へ混ぜず、結果の総 byte 数が不明なら回収コストも unknown のままにする。

S15c の常駐入力は既存 user config の隣にある private `coordinator.json` に保存し、既存 bootstrap input を再利用する。`config.toml` の文法や暗黙の環境変数は追加しない。未初期化は明示エラーとし、`ls` / `show` は停止中でも読み取り専用 DB を使い、起動・epoch 更新・migration を行わない。`submit` だけが必要な owner 起動を要求する。

S15c の残件には、file/reader ベースのメモリ上限付き source 転送、active attempt のページ走査、requires/profile の配置条件、success/failure/always と glob・複数 path の出力計画を含める。現在の限定的な submit 経路でこれらを明示拒否していても、完成条件からは外さない。終了済み worker の spec・workspace は回収 ACK 前にも削除し、保持対象の結果・ログは残す。親側 source/Vault の retention は S15e で retry と回収条件を保護して接続する。

S15c は source/secret capture、CLI 終了後も動く local durable submit、bounded な offline query を実装・独立レビュー済み。blob/bundle は同じ検証済み FD から最大 64 KiB ずつ送り、転送停止時は owner を保持せず専用 lease で残る IO と cleanup を保護する。関連 7 件・strict clippy が成功。PR #42（merge `2b3078c`）で main に統合済み。

S15c 残件の確認（2026-11-17、main `a5823f8` 起点）。5 項目とも実装は main にあり、この変更は不足していた検証 2 件だけを足す。src の本体は変更していない。

- file/reader の source 転送: `service::forward` が検証済み FD を 64 KiB ずつ `put_blob_reader_as` へ渡す。根拠は `source_upload_opens_verified_descriptors_without_links_or_whole_reads` と forward の unit 6 件。manifest は 1 件ずつ JSON 全体を送る（blob/bundle ではなく metadata）。
- active attempt のページ走査: secret 配送は `live_attempt_page`（16 件、上限 64）と `live_attempt_subset` を使う。直接の検証が無かったため、70 件を 16 件ずつ欠落・重複なく走査し、terminal・retire 確定・別 worker を除き、上限 64 と subset 64 件超の拒否を確かめる storage テストを追加した。Coordinator 本体の `live_attempts`（4 箇所）と配置時の `unacked_demand` は全件読みのままで、この確認の対象外である。
- requires/profile: 充足側は実 CLI の `capability_cli_named_profile_and_requires_use_actual_path_and_freeze_retry`、未充足側は `capability_filter_runs_before_forecast_and_records_unmet_requirements` と worker admission の `capabilities_reject_bypassed_placement_before_acceptance_or_workspace`。実 CLI での未充足のテストは足しておらず、coordinator と worker admission の統合テストで代替する。
- 出力計画: 実 CLI は `always` と単一 path だけを通していた。project 設定の `success`・`failure`・`always`、`**`・`*` の glob、複数 path を `exec` から `outputs` の書き出しまで通す `public_exec_output_conditions_globs_and_multiple_paths_follow_the_outcome` を追加した。条件を満たさない required 出力は欠落に数えず、exit code 23 と 0 がそのまま返る。
- remote worker の lifecycle: S15c の範囲は登録済み worker の接続所有・hello 検証・切断時の detach と SSH child の reap・再接続、および終了済み attempt の spec・workspace を回収 ACK 前に片付けることである。根拠は `two_remote_roles_deliver_inputs_and_reconnect_same_attempt_while_offline_peers_do_not_block_local`、remote の unit 6 件、`bounded_terminal_cleanup_progresses_and_retries_without_touching_running_work`（ACK 前に spec と workspace を消し、実行中の attempt には触れない）。結果・ログの保持を同じテストでは直接確かめておらず、`nothing_is_collected_as_garbage_before_ack_collected` に依る。worker の add/install・鍵登録・native service・upgrade/rollback・ephemeral は S21〜S23 の残件のままである。

A01〜A49 に S15c を担当行とする項目は無く、S15 行の「各コマンドの統合テスト」で検証する。追加 2 件の focused 実行、fmt、`cargo test` 全体（unit 1188 件・integration 576 件）は手元（Rust 1.92）で成功した。全 target の strict clippy は、1.92 だけが報告する既存の `nonminimal_bool`（3 行）を除いて警告が無い。CI と同じ stable（1.100）での clippy と test は未実行である。全体 local-ci・mutation・リモート CI は親の gate とし、この確認で S15 全体の完了とは扱わない。

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

S15a（PR #40、merge `8939ef2`）と S15b（PR #41、merge `05e981c`）は main に統合済みで、各 HEAD のローカル実行 9 ジョブ・リモート 9 ジョブが成功した。S15c の PR #42 はローカル実行 9 ジョブ成功後、Linux の失敗子プロセス fixture が startup lock の試験へ到達しない問題を検出した。専用の private executable と起動 marker へ置き換え、同じ lock・中断・再起動の検査を保って再検証し、PR #42 は merge `2b3078c` で統合済みである。

managed init、独立 worker role、配置前の source readiness、双方向に revision で fence する Cancel/Retry を、利用者の local 実行経路の区切りとして統合する。異なる設計の担当コミットと独立レビュー結果を保持し、統合時の旧 IPC fixture と非同期 adoption の観測 race は別のテスト修正で扱う。storage 86 件、resident unit 8 件、completion 4 件、coordinator 41 件、retry/secret の実 CLI、resident integration 6 件、worker role 6 件と strict clippy が統合後に成功した。転送完了を待たず durable queued を返し、source gate と接続交換は SQLite commit 完了まで同期する。全体 CI とリモート検証が終わるまで本流へ統合しない。

`--in-place`、requires/profile、条件付き・glob・複数 path の出力計画、公開 logs/outputs、結果 retention と reader pin、SSH の接続所有・再接続は実装と個別検証後、同じ S15 feature branch へ統合した。統合後の全体 CI・公開は未完了。source/Vault の保持・消去、読み取り専用 estimate と実測 cost 接続は必須残件として継続する。

統合 HEAD の全体ローカル CI は通常テスト・coverage を通過した一方、mutation の unmutated baseline で init readiness 2 件と captured submit 1 件が期限切れになった。原因は未確定で、変異の結果とは区別する。private fixture の owned child にだけ Tokio worker 数を制限し、init の終了状態・private stderr の固定分類、submit の bounded TTC ledger/sidecar metadata と worker journal を期限切れ時に残す。本番の CPU guard・期限・登録手順は変更しない。対象 3 件は各 3 回と strict clippy が成功した。診断追加後の全体 CI とリモート検証は未実施である。

### S15e の増分ログ回収

S15e1a は schema v7 の stream ごとの committed offset、GC ref の先行登録、CAS 更新、未 commit tail の回復、読取専用の prefix reader、検証済み旧 receipt の引継ぎを実装・独立レビュー済み。storage 92 件・ResultStore 14 件・migration と読取専用 CLI の関連 1 件、strict clippy が成功した。collector / resident の増分回収と公開 `logs --follow` は後続 slice で接続する。`outputs`、resident GC と reader lifetime の接続は別の必須残件であり、この基盤だけで S15e 完成とは扱わない。

S15a は PR #40（merge `8939ef2`）、S15b は PR #41（merge `05e981c`）で main に統合済み。各対象 HEAD のローカル実行 9 ジョブとリモート 9 ジョブが成功した。S15c の bootstrap / source / query slice は PR #42（merge `2b3078c`）で main に統合済み。


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


### S20 受入 A33・A42 の確認（2026-11-17）

main 上で S20 の 4 点（5 adapter の detect/resolve/install/activate、同一 toolchain の setup
singleflight、凍結契約の submit/admission 接続、`blocked`）が実装済みであることを確かめ、受入テストの
欠けを補った。A42 の導入できる側は公開 `exec` の nodenv 試験（並行版・retry・復旧）と 5 adapter の
private process 試験が、A33 の tool 不足側は worker_agent の必須 tool fail-closed 試験が既に担う。
今回、導入できない側として、operator の inventory にない版を固定した公開 submit が
`blocked: no_compatible_worker`（`capability_mismatch`）となり、command も manager も起動せず base profile を
変えない試験を `tests/it/bootstrap.rs` に追加した。実 manager・network の導入、導入中の負荷の forecast、
installed inventory、diff/sync（S21）は引き続き残件で、S20 全体の完了とは申告しない。

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


### S16 captured planning integration

`ci plan` は local-ci 0.13.0 の `build_frozen_plan_from_source` へ一度の capture の manifest と検証済み object store を渡す。`CiPlan` は同じ FrozenPlan と captured identity を保持し、公開出力は上限付きの構造 summary のみとする。captured config は manifest に載った regular file だけを読み、秘密入力・hash/size 不一致を拒否する。capture 後に live project を再読込しない。Directory に架空の Git revision を補わず、branch/path filter の不足 metadata は core の型付きエラーで拒否する。

planning は owner、epoch、attempt、ledger へ登録せず、close / Drop で自分が作った private draft と Vault だけを削除する。Git/worktree/Directory と dirty submodule の planning を検証する。初期 capture policy は既存 Context の設定解決を利用する。実 CI 実行、worker Driver、remote artifact、CI status、shard retry、強制終了で残った draft の回収はこの区切りの完了に含めず、既存の必須残件として保持する。subcommand 無しの `ci` は引き続き未実装。


### 統合検証と次の区切り（2026-11-15）

S15統合 `f94d6f1` の全体ローカルCIは9ジョブ中1成功・7失敗・1環境エラーで終了した。通常テストのHOME比較・保護PATHのfixture、短いUnix socket path、source guard最後所有者の明示unlock、bootstrapのAccepted待機を個別に調査する。mutationはbaselineで停止し、mutant検出率は未測定、coverage集計も未完了。限定pure gateの成功を全体CI成功とは扱わない。修正統合後のコミット済みHEADで全体検証をやり直す。

関連SDK local-ci 0.13.0はPR46、ローカル10項目・リモート10項目を通過し、crate・tag・4対象8配布assetの公開と照合が完了した。0.14開発のDriverは動的展開・worker再検証まで実装し、評価サイズ上限と状態容量境界を修正中。元の独立レビュー担当でproducer確定・permit解放を失わないことを確認してから、既存RunnerのDriver接続、cancel/fence、shard retry、artifact backendへ進む。

setup実測は、workerのowner世代・挿入順に束縛した最大64件pageの取得から、follow専用の親bounded committed historyまで接続した。実際のfeature交渉・worker/boot/epoch・attachment/InputGate世代を照合し、actor解放後に既存のqueue32/cache4096 writerへbest effortで渡す。親send時刻からremote ageを差し引き、TTL・native boot・clock逆行を検証する。同じreceipt・競合は初回stampを更新せず、eviction後もnamespace sequence watermarkで古い再送を拒否する。namespace台帳も4096件に制限し、同じ親native bootで満杯なら新namespaceの観測はUnknownとする。親native bootが変わった履歴はfreshnessに使わず、旧namespace slotを回収する。queue fullやwriter故障による欠落は許容し、通常placementへ失敗を伝播させない。読み取り専用estimateはpageを要求せず、cursorと履歴も更新しない。setup CostModelのscalar・forecast・warm readiness・resource/dependency展開は引き続きUnknownで、次の必須sliceに残す。準備CPU時間を単純なready delayへ足さず、既存TTCの依存nodeと実ledger entryの対応でモデル化する。warmの0は現在のartifact内容・receipt・profileが検証された場合だけに限定し、観測不足や検証budget超過はUnknownを維持する。新しいschedulerは作らない。この接続と資源モデルの検証が済むまでS15f完成とは扱わない。


### 新しい純粋ロジックの限定gate

CI計画summaryをcoverage/mutation対象へ登録し、shard group数の4096/4097境界を追加した。限定3テストでcoverage364/364行、mutant40件はcaught39・survived0・timeout0・unviable1。観測page transportは別branchの`6b5a4c1`でcoverage341/341行、mutant34件はcaught32・survived0・timeout0・unviable2、baseline5件成功を独立担当で確認した。どちらも通常閾値を維持した限定結果で、統合全体CIの結果ではない。


### 準備観測の親取り込み限定検証

follow pagination（64/64/2件と次pass）、readonly estimate、actor解放、owner reset・同世代attachment置換・InputGate無効化、malformed page、remote age/TTL/clock逆行、永続初回stamp・競合・eviction watermark、namespace上限、queue full・writer故障を確認した。関連18テスト成功後、追加境界を含むfollowの4テストも成功（重複を除き19件）。all-target型検査・strict clippy・fmt/diff確認も成功。これは限定検証で、統合全体CI・新しいwarm/resourceモデルの検証ではない。

独立レビューで、構造正常な期限切れ観測がpage全体を巻き戻し、古い先頭64件を越えられなくなるP2を修正した。identity/sequence/clock等の構造不正はpage全体を拒否し、TTL超・親native uptimeへのmapping不能な観測だけをskipしてvalid cursorを進める。64件の古いprefixからfreshな後続pageへの到達、再接続・親restart、全expired完了pass後のfresh insertを含む関連21テストとstrict all-target clippyが成功した。全体CIはこの修正では再実行していない。


### 統合CIの再検証で見つかった問題（2026-11-15）

`98945fc` の全体ローカルCIは2成功・4失敗・2取消・1skip、50分42秒で終了した。通常UbuntuとMSRV名のジョブは成功したが、ローカルRustは1.100であり1.89確認ではない。通常macOS名のbootstrapはEnvironmentMismatchによる未受理、coverageはcapability390/391行・wire1709/1710行、mutation2/3は回収fixtureの64件期待に対し63/60件でbaseline失敗した。mutation0/1は取消で、完全な変異検出結果は無い。

mutation routing traceではGit起動がfixture開始7秒後となり、5秒でready FIFO読取側が閉じていた。所有FIFOへの一時readerで自然解放したが、その実行を成功gateに使わない。ready/release両FDをstartup完了まで保持する修正を統合した。初期Git到達待ちはbootstrapの20秒契約と合わせ、executor応答の5秒watchdogは維持した。6秒遅延Gitが成功し、同期startupへ一時的に戻す退行は5.57秒で検出、最終fixtureも成功して製品sourceは復元済み。残るprofile観測・回収fixture・pure不足を修正してから、コミット済み統合HEADの全CIを再実行する。


989で失敗した3経路は修正統合`208987d`で、bootstrap・起動preflight・部分ページ回収をnextest jobs2へ絞って再実行し3件とも成功した。capabilityのinstallable bundleがprofile/OS/arch/feature条件を免除しないこととbundle無しの拒否を追加し、wire test helperの不要な型分岐を共有response値へ整理した。限定46件成功、coverageはcapability411/411行・wire1709/1709行、mutation130件はcaught117・survived0・timeout0・unviable13。profile修正は起動時Knownから一時Unknownへの変化だけを許容し、実体/版の変更と初期UnknownからKnownは従来通り再attestationを要求する。必須ツール・固定baselineはlive Unknownで拒否し、独立安全レビューでP1/P2無し。実際のCI不一致の方向は未確定なので、全体CIで再確認する。


`a665cf7` の再検証は通常Ubuntu名のbootstrapで同じEnvironmentMismatchが再発したため、16分43秒で早期取消した。通常macOS名は1604件成功、MSRV名は成功（実Rust1.100）、coverage/変異は未完了。profileのKnown→Unknown subsetだけでは全不一致の解消を証明できない。拒否規則を弱めず、実体/観測変化・起動時未知の既知化・固定baseline拒否を静的な安全分類で識別する診断を追加した。既存profile/worker拒否と機密値除外の9テスト、strict clippyが成功。次はnormal jobへ絞った同じlocal-ci環境で原因を確認し、最終の全体9ジョブは別途必須とする。

ユーザーは承認案に示した条件でsource/Vault自動消去の実装・有効化を明示許可した。親state内のみ、sourceは既定7日と全世代終了・必須回収完了・共有/pending参照無し、Vaultは終了・必須回収完了を条件に、32件以下のpage・永続claim・時計異常時の保留・旧claim再実行防止を必須とする。元ファイル/元secret/鍵/Git refsは対象外。Aのtoken/reader/fence下地は別branchで実装・安全レビュー中で、実expiry/unlinkとVault protocol/resident有効化はB/Cの必須残件。元の内容address IDを変えず、新publication incarnationと旧runの永続失効を区別する。承認の再確認は不要だが、有効化前の削除専任レビューと検証は省略しない。

### 起動時 native profile attestation の bounded warm measurement

`252c311` の normal-only local-ci は bootstrap の `profile_gained_observation` を確認して部分実行で取消した。起動時 Unknown が後の Available へ変わる方向を診断できたが、全体 gate の成功ではない。runtime 構築時の最初の report が resident fingerprint を固定していたため、cold launcher の一時 timeout がそのまま baseline になっていた。

runtime 構築時だけ `startup_report` を使い、各 tool の結果が確定するまで測ってから最終 report と baseline を作る（300ms で時間切れになった cold な native launcher を Unknown として公開し、後で応答したときに drift と誤認して worker を block していたため。ユーザー決定）。1 回の呼び出しは 2 秒まで、時間切れになった呼び出しだけを tool ごとに最大 3 回、全 tool で共有する 5 秒の範囲で繰り返す。失敗・解析不能・Flutter/Dart wrapper は確定した Unknown で繰り返さない。共有時間を使い切った後も各呼び出しは 300ms 待つので、xcode の SDK probe を含む固定 14 calls に対して startup report の待ちは最大 5 秒 + 14 × 300ms = 9.2 秒。profile 読み込み時の required tool の検査も同じ規則（最大 9.2 秒）で、確定しても available でなければ読み込みを拒否する。最初の live report の 4.2 秒と合わせた最悪値は 22.6 秒で 20 秒 bootstrap を超え得るが、それは複数の tool が 2 秒以上応答しない場合に限られ、その場合は bootstrap の明示的な時間切れになる。それでも時間切れの tool は Unknown のまま公開し、後から観測できれば drift とする規則は変えない。live report の 300ms 期限は変えていない。無制限の収束待ちや完全な遅延耐性は保証しない。

missing binary は Unavailable、再測定しても失敗する Docker 等は Unknown のままなので制約なし shell を妨げない。初回の Unknown report/fingerprint は公開せず、成功も実測値のみを使う。公開後の Unknown→Available は引き続き identity を無効化し、known version・executable・context drift、required tool と frozen baseline の fail-closed 判定は変更しない。private cold script が初回 timeout→再測定成功を決定的に再現し、再測定除去の退行が同じ availability assertion で失敗することを確認した。profile 7件と最終 toolchain 65件、strict clippy all-targets、fmt/diff check と、元の失敗対象 `bootstrap::configured_cli_submit_captures_worktree_and_secrets_then_outlives_caller` の単体実行が成功した。normal matrix と全体 CI は統合後に別途必須。

### 準備状態probeへの起動時attestation修正の統合

ROOTの起動時attestation修正3216a745を、準備状態probe branchへd2968294で統合した。最初の測定とUnknownのみの1回再測定を完了してからfingerprintとcompleted snapshotを保存する。読み取り専用probeがsnapshotの時刻を更新しない契約と、公開後のUnknown→Availableを同じidentityで受理しない契約は維持する。統合後のprobe関連10件、profile/snapshot関連8件、strict clippy全target/featureが成功した。wireへのprobe接続・warm activation証明・実測cost接続は残件で、setup時間を推定成功へ変更しない。ROOT3216の通常matrixローカルCIは別途進行中で、このfocused結果を全体CI成功とは扱わない。


### 準備状態probeの読み取り専用worker protocol

独立した `preparation_probe_v1` を交渉し、`PreparationProbe` をread-only/control operationとして追加した。実際のmanifest/source/run/setup/frozen/in_placeと選択workerのexpected profile fingerprintを受け、現在のcfg fingerprintが一致した場合だけ既存のbounded worker-local probeへ渡す。setup/frozenはDebugから除外する。応答はworker/boot/agent epochと実際のowner generation・opaque producer key・ConcretePlan・Active builder/attempt・typed Unknownのみで、最大16件をdecode時にも制限する。InspectやProfile.reportを呼ばず、completed snapshotの元の時刻とTTLを維持し、queue/attempt/ledger/native commandへ作用しない。

このsliceはworker/wireだけであり、親のquery送信・観測取り込み・generation fencing、verified activation/warm proof、実際のTTC ledger identityとの対応とcost接続は必須残件として残す。setup秒・foreground forecast・cold/warm・admissionの意味は変更しない。focused wire/probe/protocol/RPCとstrict checkの結果はhandoffへ記録し、全体CI・push・公開の成功とは扱わない。

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


### S15 source retention B: bounded explicit deletion (resident disabled)

B adds an explicit parent-only `SourceStore::retention_page`; no resident caller or Vault erasure is enabled.
Source EX is acquired nonblocking before coordinator actor work and held through synchronous physical IO/commit.
The configured writable coordinator is matched to held source/state identities and SQLite's actual main connection
via actor-local `SQLITE_FCNTL_HAS_MOVED` (unknown/unsupported/moved fails closed without opening a validation DB FD).
Exact run/source-ref digest, sealed manifest and FD-read private run binding precede a permanent run-expiry fence.
Every generation must be known/typed terminal with mandatory collection closed; queued/retry generations block.
The history proof intentionally scans a run's history, so it is not described as constant-time.
Direct create_task/create_attempt now take source SH and consult the run fence; terminal/result updates are unchanged.

Each page selects <=32 indexed memberships or <=32 pending exact claims, with separate examined/path counters.
Only exact membership PK rows are released; root/graph/object/run tombstones remain and no cascade/directory walk
is used. Incomplete/unknown/import/legacy and shared memberships retain protection. Clock Hold forbids expiry,
new claims AND replay unlink. A durable claim includes store/ancestor identity, incarnation, safe relative class/path,
leaf dev/inode/size/UID/link/mode/timestamps and verified content digest before any unlink. Root/ancestors are
NOFOLLOW-opened and checked again before FD-relative leaf unlink; directories are never removed. Data 0644 and
nested owner-controlled 0755 directories are accepted under the private 0700 source root; index/lock rules stay 0600.
Claimed NotFound alone allows parent fsync + durable completion. Errors retain claims/data. Completed claim records
are permanent, excluded from replay, and atomically reserve a higher publication incarnation before same-ID reuse.
Old run fences remain permanent. No source expiry is created for Vault-only SecretUnavailable.

Content hashing (new proof plus replay) shares 64MiB/page; a new leaf above 32MiB or insufficient remaining budget
is Unknown and retained. Initial sealed parent-manifest eligibility validation separately has a 64MiB bound and is
cached across member pages. Unproven large legacy inputs are explicitly not fully covered. Source registration still
uses the separate nondeleting preview; B Holds until bounded registration is known complete. C must thread trusted
clock/config samples, owned blocking shutdown/cancellation drain, parent-only Vault semantics and resident scheduling,
then obtain deletion-focused review before automatic deletion. No real user state is a test fixture.

B focused verification completed on private fixtures: source retention44 passed plus2 standalone-ignored owned
subprocess helpers, retry-filter25 passed, strict all-target/all-feature clippy, fmt and diff checks passed.
New tests include33/65 refs,65-submodule graph and read counters, exact owner/DB/run digest, retired-current-generation
queue exclusion, permanent directAPI fences, clock-Hold replay, claim/unlink/fsync/completion crash points,
claimed/unclaimed missing, same-ID next incarnation, root/ancestor/leaf/symlink/hardlink/mode/path mismatch,
owner-lock replacement, pending roots and sparse oversized leaf retention. Source preview and reader/IPC/async
cancellation guard regressions remain green. Full local-ci/MSRV/Linux/remote/native/runtime-GC acceptance is not claimed.

### 起動時attestation修正後の通常CI

3216a745eec89202d68cc5a61e91150a7274f016の通常matrixローカルCIは2ジョブ成功、失敗・環境エラー0、11分33秒だった。git-lfsのシステム導入とtaiki-e/install-actionは各matrixで未再現のため、CLI全体はincomplete（exit3）で、完全再現成功とは区別する。Docker cleanupはコマンド不在で確認できない警告があった。全9ジョブはこの結果に含まれず、coverage・mutationを含む全体検証を次の区切りとする。source削除Bは別branch e5eb4df6で44件とstrict検証済み、Vault Cと専任削除レビュー・常駐有効化は継続中で、この通常CIには含まれない。


### a4875b3 全体CIの部分結果とfixture・並列度補正（2026-11-15）

`a4875b3b576296d8c097e84d1366dc95d62f6393` の `local-ci run --include-disabled --parallel 1` は3470.86秒で所有実行を取消・回収した。9ジョブ中3成功（通常matrix各1606件とMSRV名）、2失敗（mutation baselineのtoolchain PID読取とworker起動待ち）、2取消error、2取消skipで、全体成功ではない。MSRV名の実toolchainはRust1.100。結果は `/private/tmp/rtr-a487-full-local-ci-partial-result.json`。coverage・mutation全gateと未再現actionは未検証のまま。

manager子PIDは一時ファイルへの書込後renameで公開する。toolchain/workerの私有fake fixtureだけにbudget2・CPU閾値100・memory reserve0のTTC設定を置き、実ledgerとquotaを維持する。workerはenrollment完了後に作成し、既存state拒否を回避しない。外側の共有CPU admissionと製品既定値は変更しない。mutationは明示されたCARGO_BUILD_JOBSを尊重し、各shardのRust test並列度を1にする。従来の4 shard×nproc12による共有予算12の超過を補正するもので、すべての起動遅延がCPU guardだけで説明できたとは主張しない。

Rust1.100で限定検証はtoolchain13件（21.83秒、1500ms deadlineと子回収assertを維持）およびworker_role6件（5.00秒）が成功。`cargo clippy --all-targets --all-features -- -D warnings`、fmt、diff-checkも成功。全体CIはこの補正後に再実行が必要。削除B/Cと純粋モジュールgateの統合・専任レビューは別の残件として維持する。


### S15e C: parent Vault の明示削除ページ（resident 未接続）

`SourceStore::vault_retention_page` を同期 API として追加した。source EX → Vault EX → owner actor の順で、
B と同じ owner/source/run/manifest digest・全 generation terminal・必須 collection closed・queue 不在を確認する。
Vault は独立した永続 `vault_clock`（retention 0、初回は Hold）を使い、source の7日保持や run_expiry を変更しない。
最大32 run/claim と64MiBの manifest/secret 検証予算を持ち、秘密の HMAC/path/size も削除 claim 前に検証する。
永続 vault_delete_claim が parent run の利用不能 tombstone を兼ね、clock Hold は新規 claim と物理 replay の両方を止める。
完了 claim は再実行せず、再作成・別 inode・内容/権限/祖先の変更は保持する。実 unlink は FD 相対 NOFOLLOW、
private UID/nlink1/0600 の厳密な run.json のみで、親 directory fsync 後に完了する。

Vault 内の private parent registration は source root/index の identity を固定し、generic constructor/has/load/adopt/capture も
この永続境界を確認する。SourceIndex meta にも marker 公開前に同一登録を永続化し、marker 欠損時は
既存 index のみを READ_ONLY で開いて分類を復元する（新規 index は作らない）。永続 Vault lock にも
marker/claim より先に同じ登録を fsync し、source tree と marker の同時欠損でも親分類を保持する。index 欠損・交換・不明は
Unavailable/has=false で、worker mode へ戻らない。secret run の source expiry/member 解除は、
その run/source/厳密 binding と一致する完了済み Vault claim まで保留する。共有 manifest の各 run に適用し、
C の clock Hold・busy・cursor 遅延・個別エラー・未完了 claim があっても必要な manifest を保持する。
直接 API と cloneable HeldBundle は Vault SH を持つ。send_bundle は opaque lease を encoded Outgoing に渡し、
caller timeout/cancellation 後も queue・write・flush/cut が所有する。writer 死亡/abort は残る queue を破棄し、
writer の内部 buffer を破棄するまで current frame の lease を保持する。last lease は明示 unlock する。
直接 adopt の初回公開は linkat による atomic no-replace で、同時要求の後発が先発 bundle を置換しない。
parent 登録のない worker の ephemeral erase/reupload は維持する。

自動 resident loop は追加していない。B+C+transport の専任削除レビュー後に D で trusted clock、bounded scheduling、
spawn_blocking の所有と shutdown/drain を接続する。実 user state、原本、key、Git refs/cache、unknown legacy の消去は行わない。
focused 検証と strict lint の最終結果は C handoff に記録し、全体 local-ci/MSRV/Linux/native/runtime acceptance は親統合で行う。


### S15e D: default parent resident Source/Vault retention

B+C+transport の専任削除レビューは ecf2f020 で P1/P2 解消済み。resident は既存 retention_days
(default 7) に従う Source と、terminal+mandatory collection closed 後の独立 retention 0 の Vault を
既定有効で処理する。最初と60秒 Skip tick ごとに登録/Vault/Source の単一 bounded page、登録完了後は
Vault/Source を交互に実行し、同一 tick で両方32件を処理しない。登録は別フェーズで最大32 directory entries
または32 refs、未知 entry のエラー時は traversal を再開せずリセットし、oversized/unknown は保持する。
Source/Vault の新規 claim は legacy done・unknown pending root 不在・incomplete graph 不在まで global Hold する。
無関係な terminal Vault run もこの既存 C 条件で保持し、既存の厳密 Vault claim のみ trusted clock 下で replay できる。
bounded legacy reconciliation は既存計画の未完了項目として残す。各ページの新規・replay 上限32と64MiB検証予算、独立 persisted clock/Hold、全 generation・queue・
厳密 owner/binding・Source の完了済み Vault claim 条件は変更しない。原本/key/Git/cache/unknown extra は消去しない。

Coordinator と CapturedInputs の Arc/cursor/guard を owned_blocking closure が保持し、shutdown は実作業完了まで
待つ。actor/admission/transfer mutex 上で filesystem IO は行わない。fenced owner は page を受け付けず、optional GC の
unknown/IO エラーは data/claim 保持と sanitized counter 診断で次の tick に再試行する。結果GCの動作変更はない。
既存 generation-history proof は全履歴走査であり、page件数/byte bound から定数CPU時間は主張しない。

private fixture で actual automatic Vault cleanup・reader busy・未提出/incomplete collection 保持・未知legacy保持、
helper の deterministic sample で default Source7日/Vault0 独立・65 files 最大32/page と scope sentinel、
actual resident で SourceEX/VaultEX 内 blocked page の IPC/renewal 継続と shutdown/owner/fence drain を確認する。
128 terminal task fixture の status+tick latency は実測サンプルのみ。専任 D レビューと親の最終 local-ci は
この実装 handoff 後であり、本作業で実 user state、native service、SSH、push、merge は実行しない。

D 最終 focused 検証: resident 23件成功 (88.35s)、retention filter 69件成功 +2 standalone owned-helper ignore
(28.36s)、strict all-target/all-feature clippy 成功 (12.35s)。128 terminal-task fixture の最大 status+tick latency は
1.287625ms のサンプルで、任意履歴の最悪時間や定数CPUを保証しない。既存 estimate readonly fixture は Source SH
で背景 GC を隔離した上で whole-source-file/DB snapshot を維持確認する。原本や実 user state は一切対象にしていない。

### S15d・S15e の確認（2026-11-17、main `7cbe913` 起点）

行の完了条件を句ごとに「実装済みかつテスト済み／実装済みだがテストが無かった／未実装」に分けた。未実装は 1 件（Vault の公開途中の停止からの回復）で、この変更で実装した。テストが無かった 4 件にはテストを足した。それ以外の src は変更していない。

S15d（すべて実装済みかつテスト済み。この変更では触っていない）

- `exec`・`wait` の exit code の透過: `tests/it/bootstrap.rs:1396` `configured_control_exec_exit_and_offline_wait_ignore_current_project_and_git`、計算は成功で回収が欠けた場合の 78 は `:1765` `control_exec_succeeded_compute_but_missing_collection_exits_78`、判定の純粋部分は `src/service/completion.rs:277` `observes_pending_unknown_cancel_and_closed_collection_separately`。
- `cancel`・Ctrl-C で停止不明を cancelled にしない: `tests/it/bootstrap.rs:1609` `control_exec_sigint`（通常と in-place の 2 件）、`:1567` `control_wait_interrupt_detaches_without_cancelling_work`、`src/storage/tests.rs:3458` `run_cancel_is_atomic_idempotent_and_preserves_unknown_and_natural_completion`。
- unknown の retry を拒否し、元 snapshot と履歴を保つ: `src/storage/tests.rs:3786` `retry_same_run_rejects_unsafe_attempts_and_unavailable_inputs_atomically`、実 CLI は `tests/it/bootstrap.rs:1803` `retry_same_run_cli_uses_original_inputs_and_restart_receipt`。
- `--in-place` を remote に配置しない: `tests/it/coordinator.rs:2631` `in_place_scheduler_uses_only_explicit_parent_local_and_never_automatic_retry`、`tests/it/worker_agent.rs:3899` `in_place_is_refused_by_default_worker_admission_before_acceptance`、実 CLI は `tests/it/bootstrap.rs:2616` `in_place_exec_updates_original_directory_and_retries_mutable_input_after_restart`。
- 未了（親の gate）: 統合 HEAD の全体 local-ci とリモート CI、remote worker を使った検証。S15d1 に記した owner 再起動後の `wait`（`tests/it/bootstrap.rs:1698`）の並行実行時の timeout は未帰属のままである。

S15e（実装済みかつテスト済み）

- `logs` の非 UTF-8・offset・切断再開: `src/service/logs.rs:687` `snapshot_raw_bytes_prefix_boundary_ndjson_and_readonly`、`:749` `follow_sweeps_resume_without_duplicates_and_close_incomplete`、回収側は `tests/it/collection.rs:1919` `active_raw_logs_are_durable_before_terminal_and_reconnect_at_committed_offset`。
- `outputs` の path 衝突・部分失敗・回収失敗: `src/service/output_export.rs:427` `tar_long_paths_dotfiles_and_partial_conflicts`、`:609` `concurrent_publish_has_exactly_one_winner`、`src/service/outputs.rs:593` `closed_failed_collection_with_unreceipted_output_exits_collection_failed`。
- output planner（条件・glob・複数 path）: 純粋部分は `src/outputs.rs:101` `conditions` と `:124` `paths_and_globs`、実 CLI は PR #57 の `tests/it/bootstrap.rs:4505` `public_exec_output_conditions_globs_and_multiple_paths_follow_the_outcome`。重複するテストは足していない。
- 結果 GC と reader の寿命: 常駐 loop は `src/service/resident.rs:772` から `src/results/retention.rs:33` の `page` を呼ぶ。reader との排他は `src/results/pin.rs:118` `shared_readers_exclude_gc_and_inode_survives_release`、`src/service/outputs.rs:876` `invocation_pin_excludes_gc_during_raw_tar_export_and_final_write_flush`、`src/service/resident/tests.rs:1421` `offline_submit_publishes_reader_pin_before_immediate_logs_follow`、実 resident での競合は `:92` `gc_pin_contention_keeps_resident_ipc_and_lease_renewal_alive`。claim 済みの読み出し拒否は `src/service/logs.rs:1113` と `src/service/outputs.rs:1011`。
- 実行中・回収中・保持対象を消さない: 結果は `src/storage/tests.rs:5158` `result_retention_keeps_unknown_unacked_open_and_pinned_references` と `tests/it/collection.rs:1344` `collected_results_are_kept_for_the_retention_and_then_deleted`。Source は `src/source/retention.rs:2807` `deletion_rejects_foreign_owner_moved_database_and_shared_reader`、Vault は `:4004` `vault_fresh_exact_owner_digest_queue_and_collection_proofs_are_required`。
- 件数上限: 結果は 1 ページ 32 件（`src/storage/tests.rs:4978` `result_retention_claims_bounded_pages_and_replays_only_known_claims`）、Source は `src/source/retention.rs:2628` `deletion_pages_33_and_65_preserve_original_and_metadata`、Vault は `:3741` `vault_page_is_bounded_and_requires_terminal_exact_proof`、常駐の driver は `src/service/resident/input_retention.rs:361` `default_source_seven_days_and_vault_zero_have_independent_clocks_and_exact_scope`（65 files を 1 ページ最大 32）。
- 時計の跳躍: 純粋部分は `src/results/clock_policy.rs:209` `forward_jump_requarantines_then_follows_the_new_wall` ほか 6 件、Source は `src/source/retention.rs:2719` `deletion_clock_holds_also_stop_physical_claim_replay`、Vault は `:3685` `vault_claim_replay_clock_holds_and_changed_target_stays`。
- Source と Vault の削除の常駐への接続: `src/service/resident.rs:801` が `src/service/resident/input_retention.rs:47` の `Driver::page` を 60 秒ごとに 1 ページ呼ぶ。実 loop 経由は `:408` `automatic_loop_registers_and_erases_only_closed_vault_copies`（Vault）と `src/service/resident/tests.rs:97` `input_gc_blocking_page_keeps_ipc_renewals_and_owner_until_stop_drain`。Source の 7 日経過は実 loop では待てないため、loop が呼ぶ `Driver::page` に時計の sample を渡す `:361` で確かめている。
- 削除前の所有確認と途中の停止からの再開: Source は `src/source/retention.rs:2681` `deletion_claim_crash_boundaries_replay_only_durable_exact_claims` と `:3046` `deletion_ancestor_swap_claim_identity_and_missing_notfound_rules`、Vault は `src/secrets/vault/guard.rs:550` `vault_exact_leaf_rejects_symlink_hardlink_and_missing_claim_recreation`、秘密付き run の Source を Vault より先に消さないことは `src/source/retention.rs:3373` `source_gc_preserves_secret_proof_before_vault_and_pending_claim`。

S15e（未実装だったもの。この変更で実装）

- retention L1（PR #45 のレビューで先送り）: 親が `Lease::publish` の途中で止まると、Vault に平文の一時ファイルが残り、link 後なら bundle の名前が 2 つになって読み取りがすべて拒否され、その run の Vault と Source の削除も永久に進まなかった。親では一時ファイルの掃除（`sweep_stale_tmp`）が一度も呼ばれていなかった。`src/secrets/vault/guard.rs:352` の `Lease::sweep_orphan_tmp` を足し、`vault_retention_page` が排他 lease・祖先・owner・登録の確認の後、時計の Hold と旧入力の保留より前に、1 ページ最大 32 件を消す。Vault の一時ファイルを作るのは lease を持つ `publish` だけなので、排他 lease を取れた時点の残りは書き手のいない残骸であり、経過時間と時計を条件にしない（C 節の「clock Hold は物理 replay も止める」は保持対象の claim の話で、どの run にも属さない未公開の一時ファイルには当てはめない）。消すのは、名前が `.<run>.json.<ulid>.tmp` か `.parent-retention.json.<ulid>.tmp` に厳密に一致し、symlink を辿らずに見て自分が所有する 0600 の通常ファイルで、link が 1 つ、または 2 つでもう一方がその公開先と同じ inode のものだけである（2 つの場合に実際に回復できるのは run の bundle だけで、理由は下に記す）。root FD 相対で unlink し、root を fsync する。それ以外は残して数え（当初は unknown に含めた。レビュー対応の L1 で別の数に分けた）、一時ファイルを bundle へ昇格させない。
  - テスト: 名前の判定 `:677` `orphan_tmp_target_accepts_only_publication_temporaries`、link の前後で止まった 2 通りからの回復と共有 lease での拒否 `:707` `orphan_sweep_recovers_a_publication_that_died_before_or_after_the_link`、symlink・directory・別 inode の hard link・別の公開名への link・0644 を残すこと `:744` `orphan_sweep_retains_whatever_is_not_provably_a_dead_publication`、33 件が 32 件と 1 件に分かれること `:783` `orphan_sweep_is_bounded_per_call_and_resumes`（以上 `src/secrets/vault/guard.rs`）。ページ経由は `src/source/retention.rs:3770` `vault_page_recovers_crashed_publication_despite_holds_and_then_claims`（reader がいる間は busy で 0 件、時計の Hold 中でも回復し、回復後に通常どおり claim して消す）。実 loop 経由は `src/service/resident/input_retention.rs:536` `automatic_loop_recovers_a_crashed_vault_publication_while_deletion_is_held`。
  - 再起動から最初の Vault ページ（最大で約 60 秒後）までの間、名前が 2 つの bundle の読み取りは拒否される。コードを読んだ限りでは、delivery は成立せずに次へ回り（`src/service/resident/delivery.rs` の `fail_preference`）、retry は `SecretUnavailable` になる（`src/service/resident.rs` の `vault.load`）。この窓の挙動を直接確かめるテストは足していない。
  - 親の分類を記録する `parent-retention.json` が名前 2 つのまま残った場合（初回の登録の公開途中で止まった場合だけ）は、当初は回復できず常駐の起動も失敗した。PR #61 のレビュー対応（下の節の M1）で回復できるようにした。

S15e（実装済みだがテストが無かったもの。この変更でテストを追加）

- retention L5（同じく先送り）: state dir の祖先に symlink があると Source と Vault の削除が止まる、という指摘。止まるのは仕様どおりである（削除は祖先を NOFOLLOW で開き直して対象を確かめる。path を解決して受け入れると、起動後の差し替えと区別できない）。正しい直し方は「起動時に明示して拒否する」で、これは `validate_components`（`src/service/resident.rs:1536`）として、常駐の起動（`private_dir`）と `init` の directory 作成（`directory_with`）にすでに入っていた。bootstrap の `validate_paths`（`src/service/bootstrap.rs:108`）と `init` の path 解決（`src/service/init.rs:66`）も同じ関数を呼ぶが、こちらはコードを読んで確かめただけで、テストは足していない。したがって指摘の筋書き（symlink 配下で常駐が動き続け、削除だけが永久に止まる）は公開の経路からは起きない。祖先が symlink の場合のテストだけが無かった（既存は state dir 自体が symlink の場合）。src は変更していない。
  - 追加: `tests/it/resident.rs:476` `resident_rejects_a_symlinked_state_ancestor_before_creating_any_state`（親と worker の state、新規と既存の両方で拒否し、実体側に何も作らない。同じ場所を symlink なしで指せば起動する）、`src/service/init/tests.rs:137` `directories_are_never_created_through_a_symlinked_ancestor`。
  - 起動後に差し替わった場合: Source は既存の `src/source/retention.rs:3201` `deletion_symlinked_state_ancestor_never_creates_expiry`。Vault 側が無かったため `:3822` `vault_symlinked_state_ancestor_holds_the_page_and_keeps_every_file` を足した（claim も unlink も一時ファイルの掃除も行わず、元に戻せば次のページで再開する）。
  - spec 第 22 章と結果 retention の節に、上の 2 点（公開途中の停止からの回復、state path の symlink の扱い）を書いた。
- 常駐の driver での時計の跳躍: ページ単位の検証はあったが、常駐が呼ぶ `Driver::page` で Source の 7 日が短くならないことの検証が無かった。`src/service/resident/input_retention.rs:477` `wall_jump_unknown_boot_and_reboot_never_shorten_automatic_source_retention` を足した（最初の quarantine が明けた直後に終わった run を使い、壁時計だけの前進、boot 不明、再起動のいずれでも消さず、再起動後に一貫した時計で 7 日経てば消す）。

未了（この確認で完了と扱わないもの）

- 統合 HEAD の全体 local-ci・mutation・リモート CI と remote worker での検証（親の gate）。
- 既存の節に記した残件: 未登録の旧入力（unknown legacy）の bounded reconciliation（残っている間は Source と Vault の新規 claim を全体で保留する）、received root の永久保持、private in-place binding の GC。
- PR #45 のレビューの L2〜L4（retire 済みで非 terminal の attempt と結果 GC、旧版の `pending_delete` 行、`retention.lock` の書き込み途中の停止）はこの行の依頼に含まれておらず、扱っていない。
- state path の symlink を拒否する方針のため、`HOME` が symlink を経由する利用者は、`HOME` を実体の path にしないと `init` できない（LaunchAgents が常に `HOME` の下にあり、service の directory は symlink 越しに作らないため、`XDG_STATE_HOME`・`XDG_CONFIG_HOME` を実体の path にしても回避できない）。path を解決して受け入れる方針への変更は、managed service の結び付け（state path の一致）と全入口に及ぶため、この行では行っていない。
- 結果 GC のページは最初に失敗した claim で抜けるため、消せない claim が 1 つある run では、同じ run の後続の claim も進まない（ほかの run は進む）。
- worker 側の `init`（`src/service/worker_service.rs` の `Paths::resolve`）は `HOME` と `XDG_CONFIG_HOME` を従来どおり直接検証する。この行の範囲外で、変更していない。

PR #61 のレビュー対応（2026-11-17。削除専任のレビューの medium 2 件・low 3 件。この節より上の行番号は対応前のもの）

- M1（`parent-retention.json` が名前 2 つで残ると常駐も CLI も起動できない）: `src/secrets/vault/guard.rs` の `Lease::marker` を足し、`parent_registration` と `register` の両方がこれで marker を読む。pin（`retention.lock` の中身。marker の公開より前に書いて fsync してある）がある場合に限り、marker の link が 2 つで、もう一方の名前が同じ directory の `.parent-retention.json.<ulid>.tmp` で、symlink を辿らずに見て同じ inode のときだけ読む。内容が pin と一致することは従来どおり確かめる。読むだけで、`Vault::open` に unlink の経路は足していない。一時名は、その後の Vault ページの `sweep_orphan_tmp`（位置は変えていない）が消す。run bundle は名前 2 つのままでは読まない。`Vault::open` は `check_parent` の原因を捨てず、`Unavailable` の文言に理由（marker が名前 2 つ、pin と不一致など）を含める。
  - テスト: ページ経由 `src/source/retention.rs` `vault_registration_marker_left_with_two_names_opens_and_the_next_page_repairs_it`（開くだけでは何も消えず、次のページで一時名が消えて link が 1 つに戻り、同じページで保持の削除も進む）。拒否のまま残る 7 通り `vault_registration_marker_with_any_other_second_name_stays_refused`（一時名の形でない名前、Vault の外の名前、一時名の形の symlink、一時名の形の別 inode、名前 3 つ、0644、pin なし。どれも理由つきで拒否し、何も消さない）。link 数の許容の単体 `src/secrets/vault/guard.rs` `a_file_with_two_names_is_read_only_where_two_links_are_allowed`。実際の常駐 `src/service/resident/tests.rs` `resident_starts_on_a_registration_marker_left_with_two_names_and_repairs_it`（1 回目の常駐が登録し、名前 2 つにしてから 2 回目の常駐が起動でき、起動では消えず、Vault ページで一時名が消える）。
- M2（結果 GC のページのエラーで常駐全体が止まる）: loop を `src/service/resident/result_retention.rs` の `run` に切り出し、入力側の loop に揃えた。ページのエラーでは止めず、claim とデータを残して失敗を数え、原因（path や locator を含みうる）を出さずに件数だけログし、次の周回で再試行する。fence を失ったら入力側と同じく loop を終える（常駐を止めるのは tick の loop）。fence を失った分岐を通すテストは、入力側と同じく無い（テストから fence する手段が無い）。
  - テスト: `src/service/resident/result_retention.rs` `a_claim_that_cannot_be_deleted_neither_stops_the_loop_nor_the_other_runs`（attempt の directory が symlink に差し替わって消せない claim が 1 つあっても loop は止まらず、ほかの run の結果は消え、消せない claim・元のデータ・symlink の先は残る。loop を作り直しても止まらず、directory を戻せば claim が完了する）、`a_failed_page_is_counted_without_its_cause`、`a_stopped_resident_ends_the_loop_without_a_page`。実際の常駐 `src/service/resident/tests.rs` `result_gc_claim_that_cannot_be_deleted_keeps_the_resident_and_its_work_running`（消せない claim がある状態で、ほかの run の GC・lease の更新・回収・IPC が続き、shutdown まで常駐がエラーで終わらない。実タスクの実行ではなく、制御した worker link での lease 更新と回収の継続で確かめている）。
- L1（消せない一時名の残骸が保持の失敗として数え続けられる）: `VaultPage` に `tmp_retained` を足し、`unknown` に混ぜるのをやめた。常駐の loop は `Tally` で残骸の数を失敗とは別に持ち、ログは最初の 1 回だけ出す。テスト: `src/service/resident/input_retention.rs` `leftover_temporaries_are_counted_apart_from_failures_and_logged_once`、`vault_page_reports_a_leftover_temporary_as_a_leftover_not_a_failure`。
- L2（1 つのエントリの失敗で Vault の削除が全部止まる）: `sweep_orphan_tmp` は、エントリごとの stat・unlink の失敗を「残したもの」に数えて続ける。ページを失敗にするのは `current()`、directory の列挙、root の fsync の失敗だけ。テスト: `src/secrets/vault/guard.rs` `orphan_sweep_counts_an_entry_it_cannot_examine_or_unlink_and_goes_on`、ページ経由 `src/source/retention.rs` `vault_page_goes_on_past_a_temporary_it_cannot_unlink`（消せない 1 件を残し、ほかの一時ファイルの掃除と保持の削除が同じページで進む）。
- L3（symlink の拒否が対象も対処も示さない、`HOME` を一律に検証する）: `validate_components` のエラーに、symlink だった要素の path と「実体の path を指定する（state なら `HOME` か `XDG_STATE_HOME`）」という案内を足した。`init` の `Paths::resolve` は `HOME` そのものではなく、実際に使う path（LaunchAgents、設定、state）を検証する。受け入れる・拒否する path の範囲は対応前と同じで、変わったのはエラーの文言と、検証の対象を使う path として明示したことだけである（`XDG_STATE_HOME`・`XDG_CONFIG_HOME` を実体の path にすれば、使わない `HOME` 配下の既定の path が symlink でも `init` できる点は対応前から同じで、テストで固定した）。state path の symlink を拒否する範囲は変えていない。テスト: `src/service/init/tests.rs` `a_symlinked_path_is_refused_by_name_with_the_way_out`、`home_is_checked_only_through_the_paths_made_from_it`、`initialize_through_a_symlinked_home_creates_nothing`。
  - 残る制限: `HOME` 自体が symlink を経由する場合は、XDG を実体の path にしても拒否する（未了の節に記した）。この対応で新しく `init` できるようになる利用者はいない。LaunchAgents を symlink 越しに許すかどうかは未決定である。相対の `XDG_CONFIG_HOME` を `init` が拒否する点は変えていない。
- spec: 第 22 章の「公開途中の停止からの回復」、結果 retention の節、state path の symlink の節を上の内容に合わせた。

検証（レビュー対応後、手元、rustc 1.100.0）: `cargo fmt --check` と `cargo clippy --all-targets -- -D warnings`（`-A` なし、tests/it を含む）は成功。`cargo test` を 1 回通しで実行し、lib は 1272 件成功・1 件失敗（ignored 7）、失敗は `supervisor::tests::another_process_writes_while_this_one_reads_then_crashes_and_resumes`（別プロセスの spool 書き込みを 10 秒待つテストの時間切れ。この変更が触れていない箇所で、同じホストでほかの作業が並行していた）。lib の失敗で `cargo test` が止まったため、integration（`cargo test --test it`）は続けて別に 1 回実行し、588 件すべて成功（ignored 3）。bin と doctest は実行していない。レビュー対応で追加・変更したテストはすべて成功した。mutation と全体 local-ci・リモート CI は親の gate で、この対応では回していない。

検証（レビュー対応前の HEAD `7a1658c`、手元、rustc 1.100.0）: `cargo fmt --check` と `cargo clippy --all-targets -- -D warnings`（`-A` なし、tests/it を含む）は成功。`cargo test --lib` は 1259 件すべて成功（ignored 7）。integration（`cargo test --test it`、588 件）は 1 回通しでの全件成功を得られていない。実行中の load average は 20〜25 だった（同じホストで他の作業が並行していた）。1 回目（既定の並列度）は 9 件、2 回目（`--test-threads=4`）は 1 件が失敗し、失敗はすべて `bootstrap::` の実 CLI・実 owner を使うテストの待ち時間切れ（`Elapsed`）で、この変更が触れていないテストである。1 回目の 9 件のうち 8 件は `bootstrap::` だけの再実行で成功し、2 回目の 1 件（`public_exec_nested_git_cwd_uses_snapshot_root_and_preserves_in_place`）も単独では成功した。`control_exec_succeeded_compute_but_missing_collection_exits_78`（20 秒の期限）は単独でも 3 回中 2 回失敗し、src と tests を main `7cbe913` に戻した状態でも 4 回中 1 回失敗した（成功時も 7〜20 秒かかる）。この変更に由来しない、負荷下の既存の時間依存として別件に分ける。追加・変更したテスト 11 件はすべて成功した。

### 準備 identity の元 sample freshness と stored-task preview 接続

未公開の `preparation_probe_v1` を、normal profile report 完了時だけ採番する非再利用 sample sequence・native boot・元の completed monotonic stamp/5000ms TTL と、probe 最終 observed stamp を含む契約へ拡張した。completed snapshot が一時的に無くても sequence は戻さず、overflow は Unknown とする。二度の resolver が同じ実 sample を使い、全 IO 後も native boot・clock・元 TTL を再確認する。query は report/native command を呼ばず、sample を更新しない。

`estimate --task ID` は、保存済みの完全 TaskSpec/source/run と選択 worker の preview profile/boot/epoch が揃う場合だけ、hello で feature 交渉済みの同じ attachment に一度だけ readonly probe を送る。command-only・source 不明・古い peer は Unknown のままである。IO 中は actor を保持せず、応答後に attachment/InputGate 世代・worker identity・owner/input/profile binding を再照合する。最大256 worker の private identity context、同時 query 全体1件、送信 input 256KiB と最大16 entry の上限を保つ。親 send 時刻から元 sample の age と observation TTL の最小残量を使い、受信から5秒を作らない。同じ sample の再送は最初の deadline を延長しない。identity の確認だけを preview assumption に表示し、setup scalar・ready delay・admission は変更しない。

現在の producer Active は実 BuilderId/attempt の観測であり TTC command node の証明ではない。artifact の bounded 完全検証・正確な frozen activation の証明、実 TTC ledger identity と依存/resource graph の対応、既存 simulator への接続とユーザー経路の受入は引き続き必須残件である。ConcretePlan/producer 不在から cold/warm/0 を作らず、setup cost は Unknown を維持する。


### Source/Vault retention と準備 freshness の親統合検証

承認済みの自動消去条件に沿う D と、元 profile sample に束縛した preparation preview を親へ統合した。
削除専任レビューと D 専任レビューで P1/P2 は解消済み。親 ef7721f の focused integration 11件、
blocked GC 中の実 IPC 1件、freshness 統合後37件が成功した。独立した最終検証で strict all-target/all-feature
clippy と clean LLVM 計測51件が成功し、exact file coverage は sched175/175、
preparation_freshness73/73、wire1823/1823。sched 全27変異は25 caught、2 unviable、
0 missed、0 timeout（104.5秒）。対象は private fixtures であり実 user state の GC は実行していない。

これらは全体CIの成功とは区別する。次にコミット済み候補の全9ジョブを local-ci で再検証する。
ローカルの実 Rust は1.100であり、1.89の実MSRV確認・installer actions・独立Mac固有の受入は未検証。
S15f の完全artifact/activation証明と実TTC依存/resource graph、S16以降、Docker受入と公開は必須残件。

### 小さい complete tool artifact の bounded 観測と exact prospective activation

未公開 preparation_probe_v1 に VerifiedAvailable（完全な tool 内容をこの query 中に検証した sample）と
verification START を追加する。元 normal profile sample・owner/input/attachment/InputGate の fence を維持し、
親 deadline は親 SEND と元 profile/verification START の残 TTL から計算する。再送で期限を更新しない。
VerifiedAvailable は admission/reservation や Warm/準備時間0を意味しない。実 admission の検証は引き続き独立に行う。

query 全体（両 resolver・external・receipt・tree・shell）で64MiB、1 file16MiB、receipt1MiB、最大256 retained paths、
1024 visited entries、256 pending paths を上限とする。FD-relative NOFOLLOW で全祖先を開き、完全な tree hash と
owner/mode/nlink・inode・mtime/ctime・path-to-FD binding・private directory inventory を最後まで検証する。
コンパクトな receipt inventory も deserialize 中に制限し、失敗した read/growth sentinel の byte 消費を戻さない。
symlink を含む tool tree、上限超過、大きい/部分的な artifact は Unknown のままである。query は native command、
get/detect/materialize、pin、usage、attempt、lease を作成しない。filesystem IO は Setups ownership mutex の外に置く。
owner ごとの単一 try-lock は実 IO 終了まで保持し、timeout 後の重い detached query の積み上がりを防ぐ。
blocking filesystem syscall の壁時計上限は保証せず、元 TTL を越えた reply は破棄する。

VerifiedInstallation は Deserialize/外部 constructor を持たず、同じ live query budget への lifetime と identity を要求する。
通常 activate_set の IO validation と同じ順序の shared composition を使い、frozen complete set、baseline、shell、
protected environment、tool 順序、receipt digest と execution fingerprint の計算を共有する。この activation から
実 prepare と同じ workspace_key_context/template_key を計算する。workspace template の availability 自体は
引き続き identity-only/Unknown であり、setup scalar・ready_delay・admission は変更しない。

実 clock の private fixture と read-only real rustup binary（11,031,216 bytes、実行なし）による6回の校正では、
両 resolver を含む charged bytes33,428,886/query、524 read calls、27 retained paths、19 visited entries、
in-process roundtrip977〜988ms、元 profile age0→977〜988ms、Unknown0/6だった。
32MiB では125,546 bytesしか余裕がないため total64MiB/file16MiBを選んだ。
first observed read と5 repeatであり OS cache eviction は制御していない。tiny admitted payload の測定で、
network/RPC・実際の大きい Rust installation・一般的な warm 性能の測定には外挿しない。

最終 focused 検証は preparation/adapters/wire/protocol150件成功＋明示手動 calibration1件ignore、
別途 toolchain/preparation106件成功＋同ignore、strict all-target/all-feature clippy成功（Rust1.100）。
calibrationは別の明示コマンドで6 sample成功。実5.01秒 IO pause による元 profile expiry、二重 query拒否、
ownerを実drainまで保持してgateを解放、same-size/restored-mtime・ctime・root/receipt入替・hardlink/symlink・
receipt/dir flood/共有byte上限の否定、actual admitted receipt と activate_set の一致を確認した。
実 worker の negotiated readonly/profile-bound RPC1件も成功。全 local-ci/MSRV/Linux/独立レビューと新 wire/pure gate は親統合の区切りで行う。

大きい native install、template complete-content proof、実 TTC ledger ID と依存/resource graph/simulator、
実 user-path setup cost と Docker 受入は必須残件で、S15f 全体の完成とは扱わない。

独立レビューは9c963739に actionable P1/P2なし。追加で元profileとverification STARTの
包含関係を明示した。profile TTLは正確に5000ms、completed ≤ START ≤ observed なので、
verification ageはprofile age以下となり、同じ5000msの検証期限は既に元profile期限に含まれる。
STARTの前後範囲の検証を維持し、冗長なmin計算のみ整理する。受信から期限を作ることや、
元sampleの更新、期限の延長はしない。この追加差分のfocused pure gateは親が再検証する。


### 1e7a37e 全体検証の失敗と次の統合候補

全9 local-ci は44分39秒で中止（exit130）。1成功、5失敗、1中止、2未実行だった。
通常2matrixとmutation baseline3本で source guard/SQLite競合などを確認し、最後のbaselineでは
pin testの親子待機が停止した。全4shardともbaseline段階で変異実行は0件。完了した成功ゲートとは扱わない。
全所有プロセスと固有run pathの孤立daemonを照合して回収し、残存0を確認した。
pin fixtureの単一thread libtest出力prefixとstdout ready判定の衝突、同種supervisor fixture、
Source/Vault GCと通常source利用の競合を、ログ根拠に沿って別々に修正・再検証する。

一方、独立レビュー済みの小さいtool検証を次の候補へ統合する。Cost bd26449の親focused検証は
46件成功、strict all-target/all-feature clippy成功、exact coverageはfreshness95/95・wire1827/1827、
全87変異は75 caught・12 unviable・0 missed・0 timeout。正常な同ミリ秒START/observed境界の
テスト不足1件を追加して閉じた。実 user stateのGCやnative manager実行は行っていない。
この結果も新しい統合候補の全体CI・実MSRV1.89・Linux/remote受入の代わりにはしない。


### 単一スレッドのmutation baselineで止まるfixtureの修正

1e7a37e の全体CIは最後のmutation shardの無変異baselineで停止した。pin helperが実lockを
保持したままstdin releaseを待ち、親はlibtest stdoutのready行を待つ相互待機だった。
RUST_TEST_THREADS=1 のpretty出力はtest名のprefixとhelperのreadyを同じ行に出すため、
行全体の等値判定では受信を見落とす。同じ判定を使うsupervisor spool helperも修正対象とした。
pin moduleは本featureで追加、supervisorの該当源コードはbaseと同一で、新しい単一スレッド
実行が潜在するfixture前提を露呈した。全体CIの停止は成功として扱わない。

旧コード4d1f228で両親テストを各5秒上限で因果再現した。threads1ではpin 5.076秒・
supervisor 5.064秒で停止し、threads2ではそれぞれ0.027秒・0.439秒で成功した。
再現の所有process groupだけを停止・回収し、該当binaryの残存参照0を確認した。

修正はtest-only。子ごとに異なるprivate directoryで、実pin/spool取得後にatomic ready receiptを
公開する。stdout書式には依存せず、ready/正常終了は各10秒の待機期限を持つ。OwnedChildが
error・panic時も実Child handleをkill/reapし、既reap/ECHILDのPIDへsignalしない。helpersは
通常の列挙では明示ignoreし、親がexact+ignoredで実行する。元のSH/EX相互排他・inode・
spool内容・crash/restartの検証は維持し、productionのlockやGCは変更していない。

修正後の最終focusedはthreads1/2それぞれ7件成功・2件owned-helper ignore、0.69秒/0.49秒。
unique receipt・実正常終了・ready前終了・started確認後のready timeout・完了timeout・
panic unwindの子回収を含む。strict all-target/all-feature clippy成功（12.16秒）、fmt/diff-check成功。
実Rustは1.100であり実MSRV1.89・新しい全9ジョブ・remote CIの成功はここでは主張しない。

補足レビューに従いsupervisor fixtureの200000-byte読取loopにも10秒のelapsed期限と数値のみの失敗診断を追加し、終了済みwriterの未読spoolは拒否せず元の検証を維持した。threads1/2 exact各1件成功（0.45秒/0.44秒）、lib/tests strict clippy成功（12.34秒）、fmt/diff-check成功。production変更はない。

### Source intake の並行 SQLite write 修正

1e7a 全体CIは成功ではない。open済みの同一 SourceStore に8接続が同時 ingest する
経路で database-is-locked が発生した。barrier を付けた実 API 回帰でも旧実装は0.20秒で
SQLite code5を再現し、最初のSELECT後に書込みへ昇格する Deferred transaction が原因と確認した。
書込みを行う8箇所は最初のread前に BEGIN IMMEDIATE を取得し、真のread-only handoff
precheckは変更しない。FS hashing/書込みやcoordinator actor呼出はtransactionへ移さない。
同一回帰は変更後0.25秒で成功し、保存blobの内容も一致した。Source/Vault lock・claim・clock
条件は不変。関連source保持/転送の検証と別のforeground GC競合修正を続ける。

### Foreground source 入力と optional GC の競合修正

Source EX を持つ正常なGC pageに、capture/open/submit/retryのtry-SHが重なると、
入力自体が正常でも INVALID_INPUT / COMMUNICATION_UNKNOWN へ落ちる経路を修正した。
GC・actor-sensitive API・best-effort deliveryの既存try取得は変更しない。既にactor/admissionを
離したblocking入力/preflightだけ、foreground専用の取得を使う。通常IPCの5秒timeoutを
使い切らないようlock待ちは2秒固定とし、最初にopenした永久lock FDを保持したまま同じFDで
try-SHする。root、named lockのdev/inode・owner・mode・nlinkを各回と成功後に検査し、
置換・不正・IO faultは再試行しない。open_foregroundは初期化時のSH guardをStoreと共に返し、
capture・freeze・guarded enqueue/retry commitまで保持する。永久SH pinは作らない。

明示待機が期限へ達した場合だけtyped ForegroundBusy/StorageError::SourceBusyを
既存 SOURCE_BUSY (exit71)へ対応付ける。一般IOのWouldBlock、SQLite Busy、result-pin、
expired run、tombstone、digest不一致を同じものへ分類しない。既存stop-selectのあるinput
preflightはそのままstop-awareで、cancel能力のない同期open/metadata段階のwaitは2秒以内の
協調的な待機となる。FS syscall自体のhard cancel/deadlineは主張しない。独立DB検査用FDの
追加、actor内でのsource待機、claim/clock/reader/transport条件の弱化はない。

private fixtureの実retention_pageをSource EX内で停止し、長い競合ではtyped71を返すこと、
同じ入力の再送と別入力の初回RPCは実wait開始後もpendingのままPingへ応答し、EXを実drain
するとそのままAcceptedになることを確認した。元のstalled transfer・queue2・shutdown/restart
assertionsは維持し、失敗時は実Replyを表示する。既存のGC page中のrenew/cancel・owner stop
drainとretryのsource lease安全テストも成功。lock/root/mode置換拒否、期限、guard clone最終
unlockも検証した。旧MacのAccepted不一致logは実Replyを表示しておらず、その1回の原因を
後付けで断定しない。今回の強化した同名テストと過去失敗CLI7経路は成功した。

関連unit105件成功+既存owned helper2件ignore (84.23秒)、source_capture38件を含む実CLI/
転送integration45件成功 (60.09秒)、strict all-target/all-feature clippy成功 (12.84秒)、
fmt/diff check成功。actual Rust1.100。独立レビュー、親統合後の全9CI、Rust1.89/remote/native
受入はこの修正では未実行。実user stateのGC、元ファイルやnative serviceへの操作はない。

foreground最終追記: secret-aware公開capture_runでも明示ForegroundBusyだけを71へ維持し、
一般IOの従来Internal分類は変更しない。実2秒の公開API回帰で、run/Vault/原本sentinelを
変更しないことを確認した。親の事前点検に従い、wait中のprivate Source rootについても
retained FDとnamed root双方のUID・private bits・同inodeを各回/成功後に検証する。
root chmod755の負例を追加し、既存のlock mode/lock inode/root replacement拒否も保持する。
nested755の許可範囲は変更しない。最終focused13件成功 (9.06秒)、追記後strict全target/feature
成功 (12.75秒)、fmt/diff成功。先のunit105+integration45は追記前の8adb6a5での結果と区別する。


Source競合修正5c434aaの独立安全レビューは actionable P1/P2なし。pin fixture01e45c5も
所有child・PID再利用・通知・元assert維持を独立確認済みで、spool deadline補足は0b5df11で
閉じた。両方を親へ統合し、変更したsource分類のpure gateと、統合後のfocused/全9CIを
次の区切りとする。前の1e7a37eの失敗・中止結果を新候補の成功へ読み替えない。

### 競合・fixture修正後の親focused統合検証

親22d744cで単一threadの関連unit22件成功・owned helper1件ignore、public CI plan3件・
実readonly/profile-bound RPC1件・同時blob取り込み1件が成功した（integration計5件）。
GC実EX中の入力待機/typed71/同一要求Accepted、Ping/renew/cancel、stop/owner drain、
pin/helper/spool restart・source分類・元profile期限を統合状態で再確認した。
strict all-target/all-feature clippy成功（12.90秒）。source修正5c434aaのpure分類は7件成功、
exact coverage184/184、全14変異は10 caught・4 unviable・0 missed・0 timeout。
所有targetプロセス残存0を確認し、結果はprivate fixtureのみである。
この記録を含むコミット済み候補で全9 local-ciを新規実行する。実Rust1.100とMSRV1.89・
ローカルinstaller未再現・Docker cleanupの環境制限・remote未実行は引き続き区別する。

### 450ae1f の失敗と変異検証の選択を先に修正する方針

全9CI450ae1fは64分46秒で中止、1成功・5失敗・2中止・1未実行。全体ゲート成功ではない。
Retry前forward leaseのtry-SH、transfer leaseの解放順/明示unlock、profile fixtureの
初回Known前提を追加原因として切り分けた。transfer修正10cee9eを先に取り込み、
profile fixtureは実residentと同じstartup_reportでKnown baselineを確立し、marker読取を
shell builtinへ変更した。後続のnormal reportによるUnknown拒否・実version/実行file変化の
assertは維持、productionの300ms期限と観測動作は不変更。threads1/2各3件成功(1.07/0.84秒)。

ユーザーの優先指示に従い、次の全体CI前にpure変異検証の選択と再発防止を修正する。
現状の全integration込みbaselineは成功した2分割で1262/1303秒、既定mutant上限も6310/6517秒へ
膨らんだ。compile一括化の効果とは別のtest selection問題である。通常matrix/coverageの全integrationは
維持し、pure module用の明示lib/positive module filtersで全対象mutantの検出能力を実証する。
不足を変異除外で隠さず、必要な意味あるunit回帰か限定integrationを追加する。選択の共通定義、
workflow経由と広域テスト混入の契約検査、選択・所要時間・件数の記録を加え、再発を防ぐ。

### Pure変異の選択短縮と再発防止の実証

対象49ファイルは `scripts/pure-modules.txt` に集約し、coverageとmutationで共用する。
mutationは専用runnerから明示的に `--lib --test it` とpositive module filtersを渡す。
configのlegacy/parse/pathsは既存の短いconfig integration37件を維持し、toolchainの
versions/declarationsは共有unit `toolchain::tests::` を使う。他のintegrationは実際の
`--test it --list` で検出し、config以外の名前が1件でも混ざると失敗する。
libtestのsubstring仕様でservice::outputs等のlibrary consumerも少数含むことは許容する。
通常2matrixとcoverageの全integration実行は維持した。

初回の選択suiteは47/49が100%行coverageだった。forecastのcritical-path時刻と未測定の
前方work理由、hello capabilityのnegotiated feature交差を意味あるunit2件で補強し、
最終575lib+37config IT=612件で49/49全行100%を確認した（instrumented compile込み43.97秒）。
productionや変異除外は変更していない。

全actual2656変異を単一shardで完走し、2417 caught・239 unviable・0 missed・0 timeout。
schemata実行1102.45秒（18分22秒、事前discovery/compile35.95秒は別、合計約18分58秒）で、一括prepare430.9秒、baseline8.8秒、trace612件8.6秒、
変異phase653.3秒（単独再compileを含む）。build内訳は一括2404、単独16、prepare時unviable236。
旧baseline1262/1303秒に対して約143〜148倍短くなった。state::successorsのVec::new変異は
旧heavy-test経路56.9秒に対して0.2秒でcaught。総時間は単独再compileにも左右されるため、
compile時間とtest選択の短縮を区別する。

再発防止は通常CIのpolicy testで、実runnerのargv、workflowでの直接実行への迂回、
全target/testへの逆戻り、config外ITの混入、空suite、空/重複/rename済み対象、
欠落/古い/空/未知/生存/timeout reportの誤成功を拒否する。各runは識別子を持ち、
古いreportを消去してから開始し、選択名・件数・実引数・各stage時間・mutant件数を保存する。
baselineとmutantのtest期限は固定60秒（compileは別）とし、merge gateもtimeoutを拒否する。
最終runnerの1変異smoke（97/2656）はbaseline8.3秒、trace7.9秒、state変異0.1秒caught、
compile込み64.6秒で成功した。全2656証明の既定45秒期限より寛容な60秒への変更であり、
選択suiteと変異の母集団は同一である。

検証は実Rust1.100、Cargo jobs2、TTC外側/portool内側、専有targetで実施。
local-ci0.13の公開済みsourceでは固定labelの全stepをhistory keyでTTCに載せるため、
Python runnerにもTTC二重wrapperは追加しない。3件のpolicy testとshell構文/diff確認を実施。
全体local-ci・remote CI・Linux/macOS matrix・実MSRV1.89はこの段階で未実行で、
全体ゲート成功とは扱わない。全2656の成果物は `/private/tmp/rtr-pure-mutation-all/`、
最終期限smokeは `/private/tmp/rtr-pure-mutation-timeout-smoke/` に保全した。

### Retry入力leaseの残存try-SH境界と最終解放

450ae1fのmutation baselineでは、restart後in-place Retryがsource mutation lockのraw
CommunicationUnknownで失敗し、別baselineではretry stoppingのtransfer.lock再取得が
WouldBlockとなった。前者はprepare_retryより前、Retry RPCがforward::leaseのtry-SHを
取る境界だった。後者はLeaseのlocal mutexがfile/sourceより先に解放される競合に加え、
transfer Fileにfork/dup共有descriptionを解く明示unlockがなかった。

10cee9eでtransfer lockを最終Arcで明示unlockし、Sourceとfileをlocal mutexより先に
解放する。dupを生かしたまま最終Arc後のtransfer EX/source EX再取得を検証し、Arc clone
が残る間の排他も維持した。旧実装はこのdup回帰でWouldBlockとなった。

Retryだけ専用foreground leaseへ進み、owned spawn_blockingでSource SHを取得する。
admission/actor外で待機し、同じGuardをtransfer leaseへ渡すため再取得の隙間はない。
通常配送のtry-SHは維持する。stop/abortしても実blocking取得がdrainするまでlocal所有権を
保持し、transfer待機中の停止・エラーでもSource→localの解放順を守る。typed
ForegroundBusyだけ71へ写し、他エラー・停止の従来CommunicationUnknownは維持する。

実residentのretained source/failed runに対し実GC pageをSource EX直後に止め、Retry RPCの
実foreground待機・Ping応答・2秒のtyped71を確認。その後別Retryの同じpending callが
EX解除でRetryAcceptedとなる。旧実装はこのforeground待機に到達せず回帰が失敗した。
永久lock/root/modeの置換拒否は既存foreground群で再確認し、元stoppingのowner解放と
cross-process排他assertionは弱めていない。

旧実装への新回帰2件は失敗、修正後2件は成功。関連unit51件成功（12.86秒）、実CLIの
restart/in-place/symlink/通常submitとSource captureのintegration44件成功（64.43秒）。
全target/feature strict clippy、fmt/diff check成功。実Rust1.100、専用target、承認済み
TTC外側→portool内側を使用。full local-ci・MSRV1.89・remoteは親の次gateとし、今回の
focused成功へ読み替えない。実user stateのGCやnative serviceへの操作はない。

### S22 workers add --install と lifecycle の接続（2026-11-17）

`workers add [--install]`・`start`・`stop [--cancel-running]`・`drain`・`resume` を CLI へ接続した（`Envelope::not_implemented` に落ちなくなった）。実装の分担は次のとおり。

- worker 側の運用意図は state ディレクトリの private な `control.json`（`drain`・`cancel_running`）に永続化する。agent は `inspect` の `draining` にこれを反映し（従来は常に false）、tick ごとに `cancel_running` なら live attempt を既存の cancel 経路（永続化してから supervisor が動く）で止める。読めない marker は drain として扱い cancel はしない。wire の操作は増やしていない。
- worker host の helper（hidden の `worker-service <install|start|stop|drain|resume|status>`、`src/service/worker_service.rs`）は Launcher を総称にし、版別 directory への binary 配置（sha256 検証・既存と異なれば拒否）、鍵・`WorkerSpec` の noclobber 公開、receipt 付き journal、worker 前景 role（`worker --input`）の plist、起動、agent socket の ready 確認を再開可能に行う。stop は binding で identity を確かめ、drain の後に live attempt が 0 になるまで待ち（`--cancel-running` なら cancel を要求して待つ）、所有確認つきの bootout を行う。待ちが時間切れなら drain のまま残し、再実行で待ちを続ける。すでに unload 済みなら launchctl を触らず報告し、launchd の答えが特定できなければ何も変えず失敗にする。
- 親側（`src/service/workers.rs`）は ssh の固定 argv（`ssh_exec_argv`）で、probe（Darwin かつ同 arch）→ binary の staging（stdin）→ install（鍵は stdin のみ）を行い、成功後にだけ user config へ `[[workers]]` を追記する（失敗した install は何も登録しない。再実行は冪等）。到達不能・無言・読めない答えは `COMMUNICATION_UNKNOWN`、別 worker の答えは拒否し、停止済みとは報告しない。
- 試験は偽の Launcher・偽の ssh・一時 HOME のみ。1 台上の install → start → stop は `src/service/workers/tests.rs`、agent の drain と cancel は `tests/it/worker_agent.rs`、実 binary と偽 ssh の CLI 契約は `tests/it/workers_lifecycle.rs`。本物の LaunchAgent・`~/.ssh`・sudo は使っていない。

レビュー指摘（PR #50）への対応（2026-11-17）:

- bridge の起動: install は worker の設定 dir に版に依らない `bin/current` symlink を作り、その絶対パス（`<config>/bin/current/remote-task-runner`）を応答で返す。親は `[[workers]]` の `binary` に保存し、resident の bridge と lifecycle の helper はそれを使う（`binary` が無い旧登録は従来どおり PATH の名前）。`workers add --install` の再実行で旧登録にも追記する。install の最後に、resident と同じ経路（絶対パスの `worker --stdio` を ssh で起動し hello）を通し、worker_id と cluster の一致を確かめてから登録する。
- 動いている resident への反映: resident の認証済み control に `ReloadWorkers` を足し、`workers add` の最後に要求する。resident が無ければ何もせず、失敗は結果の `resident` に「再起動まで未接続」として出す。
- staging と probe は SHA-256 を照合し、部分転送を公開しない。再実行は worker.json の揮発する env を比較から外し、receipt の無い未完了 install は置き換えを許す。local worker 名の衝突は install 前に拒否する。lifecycle は応答の cluster を照合し、start・stop・drain・resume は実状態に合わせて報告する（loaded でなければ start は失敗、停止を確認できなければ stop は失敗）。start は cancel 指示を起動前に消す。状態 dir を読めない場合は 0 件と見なさずエラーにする。
- agent は drain 中の新規受理を `Overloaded` で拒否する（再送への応答は維持）。stop は 0 件を 2 回続けて確認してから bootout する。
- 親の OS 判定は注入でき、試験は全 OS で同じに走る。

S22 の残り（必須。この変更で S22 は完了しない）:
1. 本物の launchd・実 SSH・実機での受入（A34/A35 の native 検証、S26）は未実施。偽の Launcher と偽の ssh は実挙動を確かめない。
2. drain の確認と受理の間に、非常に短い競合が残る（受理が drain の確認を通った直後に stop が 0 件を 2 回数える）。実機での stop と受理の同時試験は S26 で行う。
3. `system-user`（LaunchDaemon の `UserName`）の導入、Linux、upgrade・rollback・doctor・`workers forget`、fish / csh のログインシェル対応は未実装（S23・S24）。
4. 実 SQLite の live attempt を持つ worker に対する stop の待ち・cancel の end-to-end（agent の cancel は統合試験済みだが、helper の待ちは注入した計数での単体試験）。
