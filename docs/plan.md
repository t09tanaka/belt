# belt 実装計画

`docs/spec.md`（仕様版 1.5）の実装を、PR 単位に分けた計画である。仕様 1.5 で追加した A50 と A55 を含め、A01〜A58 の担当は下の対応表に示す。

初回公開前に製品名を再考する。現行の CLI 名は `belt`、Cargo のパッケージ名は `belt-cli`。

## 前提

- lease 更新の実プロセス試験は、準備開始・更新の durable commit・seed の更新キーと単一 monotonic 測定期限・コマンド開始・supervisor の更新キー観測を順に確認する。短い実時間枠内での起動速度や旧 watcher を期待せず、期限境界は純粋な LeaseClock 試験で検証する。観測 journal は best-effort 診断であり、lease の判定やコマンドの継続条件にしない。

- 全 PR を `main` 宛てにし、単体で CI が通る形で都度マージする。依存列の「#N」は「#N が main にマージされた後、main から起点を切る」。
- 共通の検証コマンド（belt = belt）: `cargo fmt --all --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、GitHub Actions の緑。純粋モジュールは local-ci と同じく行カバレッジ 100% のゲートと cargo-schemata の変異テストに登録する。
- 略称: belt = belt、LC = local-ci、TTC = task-traffic-controller。

## 分割表

| # | repo | タスク | スコープ | 完了条件 | 非スコープ | 依存 |
|---|---|---|---|---|---|---|
| S0 | belt | 仕様 v1.2 | `docs/spec.md`、`docs/plan.md`（この表と A 対応表） | 仕様 v1.2 を反映（workspace は fast-worktree、基準版を local-ci 0.9.0 / task-traffic-controller 0.5.0 へ、Linux と一時 worker、`.worktreeinclude`、A45〜A48）。公開禁止語の grep が 0 件 | コード | なし |
| S1 | belt | crate の骨組みと CI | `Cargo.toml`（local-ci 0.9・task-traffic-controller 0.5 を crates.io から）、§18 の module を空で全部作る、clap で §4 の全サブコマンド（未実装は `NOT_IMPLEMENTED` の構造化 error）、`--json` の外枠（schema_version・error code）、`.github/workflows/ci.yml`（fmt・clippy・test を macOS と Linux）、coverage gate と cargo-schemata の枠 | 共通の検証コマンドが緑。`belt <各サブコマンド> --json` が schema_version 付きの error を返すテスト | 各機能の中身 | なし |
| X1 | LC・TTC | core の不足調査（読むだけ・完了） | §16 の core API・§17 の公開 API と local-ci 0.9.0 / task-traffic-controller 0.5.0 の差分。結果は仕様 §16・§17 の「0.5.0 / 0.9.0 での状態」列に反映済み | 各 API が有る／名前違いで有る／無いと、必要な変更の一覧。T1・L1a・L1b・L2・L3 のスコープがこれで確定した | 実装 | なし |
| S2 | belt | 契約（ID・状態・wire） | §10 の ID 型、状態機械と terminal outcome・観測状態・receipt、error code、§11 の全操作の request/response DTO（schema_version・digest） | 純粋モジュールとしてカバレッジ 100%・変異テスト。状態遷移表のテスト | 通信・保存の実装 | S1 |
| S3 | belt | 設定 | §5 のユーザー設定・プロジェクト設定、優先順位、未知キーの error、`.local-ci.toml` の fallback と矛盾の出所表示、`[toolchain]`・`[source] secret_files`・`[[outputs]]` の型 | 設定例 2 つが通る、未知キー・矛盾のテスト | 設定の利用側 | S1 |
| S4 | belt | storage | SQLite（WAL）、migration と互換表、親 DB・worker DB の schema、条件付き terminal 確定、event journal、retire tombstone の永続保持 | 未知 version を書き換えないテスト、terminal が 1 回だけ確定する競合テスト | 配置・通信 | S2 |
| S5 | belt | protocol 転送層 | 長さ prefix JSON frame、`hello` の version・feature 照合、control を log が塞がない多重化、chunk 転送の offset・digest 再開、`worker --stdio` の Unix socket bridge、SSH 起動（argv をシェル文字列にしない） | 切断・再開・非互換 version のテスト | 各操作の業務処理 | S2 |
| S6 | belt | source capture と object store | `local_ci::capture_source` を使った worktree / commit / directory、private object store、§6 の SourceManifest、SOURCE_BUSY、未 push objects の bundle、submodule・LFS、展開時の path 安全性 | A11〜A14 相当のテスト（一時 Git repo の自作 fixture） | worker での展開 | S2（X1 で capture に不足があれば L 行も） |
| S7 | belt | worker の workspace | repo ごとの canonical clone、attempt workspace を fast-worktree で作る・消す（プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` をそのまま使う）、directory snapshot、`exec --in-place`（親 local worker 限定） | macOS と Linux で add / remove が通るテスト、元 worktree を書き換えないテスト（A48） | setup template（S17） | S6 |
| S8 | belt | secret_files | §22.3 の SecretBundle（cluster 鍵の HMAC）、親での 0600 保持と削除、転送操作、0600 配置、terminal 時と reconcile 時の消去、ログの mask | A43・A47 のテスト | toolchain | S5・S6 |
| S9a | belt | supervisor（寿命・lease・停止） | attempt ごとの別プロセス、task-traffic-controller の managed `register` / `try_admit` / `run_command`、launch intent、process identity（boot ID・開始時刻）、suspend を含む単調時計の lease、renew の冪等性、cancel の TERM→KILL と停止確認、3 種の timeout、実行中の sleep 抑止 | A20・A21・A22・A25・A26（時計）のテスト。OS 依存部分（単調時計・boot ID・process identity・sleep 抑止）の macOS 版と Linux 版（A45） | log spool（S9b） | S4 |
| S9b | belt | log spool と結果記録 | stdout / stderr の disk spool、stream ID・offset・sequence、非 UTF-8、上限 1 GiB と `log_truncated`、結果と artifact の attempt 別保存 | A29 のテスト | 回収（S14） | S4 |
| S10 | belt | worker agent | §11 の worker 側操作すべて、`attempt_id + payload_hash` の冪等受理、retire tombstone、再起動時の supervisor adopt、boot ID 変化で interrupted、`ack_collected` までの GC 保護 | A18・A19・A23・A24（擬似）のテスト | forecast（S11）・導入（S22） | S4・S5・S9a・S9b |
| S11 | belt | worker forecast | task-traffic-controller の forecast に受理済み未登録の overlay を重ねる、coverage（attempt・plan_digest・expansion_seq・node）、読み取り専用、TTL と epoch 失効、measured / fallback / unknown | A06〜A10 のテスト | 配置の判断（S13） | S10・X1 |
| S12 | belt | coordinator の中核 | durable queue（run・task・attempt）、prefetch / max_active を守る dispatch、未 ACK 需要を同じ transaction に記録、retire してから再配置、unknown の保持、epoch と再起動時の reconcile・adopt、cancel の伝達 | A19・A21・A22・A23 のテスト（親側） | 予測による選択（S13）・回収（S14） | S4・S5・S10 |
| S13 | belt | scheduler | 1 actor で配置を直列化、転送＋setup＋実行＋回収の予測完了時間で比較、同点規則、途中参加 worker の取り込み、配置理由の表示 | A01〜A04 のテスト（1 台上の複数 worker） | CI 固有の処理 | S11・S12 |
| S14 | belt | 結果の回収 | collecting 状態、artifact の digest 検証と atomic 公開、`ack_collected`、回収だけの再開、完全性検査と `no_work`、保持 7 日の GC | A30・A31 のテスト | GitHub status（S19） | S9b・S12 |
| S15 | belt | 利用者コマンド | `init`・`exec`・`submit`・`ls`・`show`・`logs --follow`・`wait`・`cancel`（Ctrl-C）・`retry`・`outputs`・`estimate`、JSON と exit code | 各コマンドの統合テスト、`exec` の exit code 透過 | `ci`（S16）・`workers`（S21〜S23） | S12・S14 |
| S16 | belt | CI 統合 | `ci` / `ci plan`、FrozenPlan を 1 回だけ作る、Job DAG の配置と needs output、dynamic matrix の展開 event、host をまたぐ中間 artifact の receipt（workflow 単位）、matrix の global permit・max-parallel・fail-fast・join・決定的な集約、分割できない理由の表示 | A10・A27・A28・A38 のテスト | test sharding（S18） | S13・S14・L1a・L1b |
| S17 | belt | setup と template | SetupRecipe / PreparedTemplate receipt、worker 内 singleflight の builder（builder_id・consumer 参照・独自 lease）、immutable template と書き込み分離の複製、`--no-cache`、`[prepare]`。fast-worktree の `[[clone]]` との境界は仕様 v1.2 に従う | A15〜A17 のテスト | toolchain の導入（S20） | S7・S9a・S10 |
| L1a | LC | step 実行の差し込み口 | `exec::Opts` に `CommandExecutor` 相当の trait（`StepRequest` を受けて `StepExit` を返す）を足す。executor が無いときは従来どおりバイナリを子プロセスで起動する。pipe fd（token の受け渡し）と process group の扱いを trait の契約に明記する。local-ci は task-traffic-controller の crate に依存しない | local-ci の CI（coverage・変異）緑、executor 未設定の挙動がバイト単位で不変（A36）、`cargo publish` まで | L1b・L2・L3 | なし |
| L1b | LC | task demand 列挙と wire DTO | `plan_task_demand`（task-traffic-controller の型に依存しない DTO。条件式・動的 matrix は `conditional`）、`SetupRecipe`・`PreparedReceipt`・`TaskResult`・`TaskInputs`・`ExecutionTask`・`JobGraph` の schema_version と digest、wire から host path を除く（`StepResult.log`・`RunResult.run_dir`）、`RunResult` の `Deserialize` | local-ci の CI 緑、既存 CLI の JSON 出力は不変（A36）、`cargo publish` まで | test sharding（L2・L3） | L1a・L2 |
| L2 | LC | test sharding（土台＋cargo test・Vitest・Jest） | `partition_tests`、adapter の discover / partition / invoke / collect / merge、履歴による長い順の割り当てと決定的な fallback、全件 1 回の集合検証 | A39・A40 のテスト（同梱 example） | Playwright・Cypress・coverage merge | X1 |
| L3 | LC | test sharding（Playwright・Cypress・coverage merge・分割禁止条件） | 2 adapter、runner 対応の coverage merge、DB / port / container を隔離できない E2E の分割禁止 | A41 のテスト、`cargo publish` まで | belt 側の配置 | L2 |
| T1 | TTC | 分散用 API の不足分 | `exec::Opts` にenv map（`env_clear` 後に設定）とstdio（stdout・stderrの追記先log path、stdinはnull）を足し、`managed::run_command` まで通す。`None` 既定で既存CLIの挙動を変えず、履歴keyにenvを混ぜない。任意: `legacy_clock`、inspect の history quality | task-traffic-controller の CI 緑、env 隔離・log 追記・lease と marker が残るテスト、既存 CLI の挙動不変、`cargo publish` まで | belt 側 | なし |
| S18 | belt | sharding の配置 | ShardGroup の配置と集約、fail-fast、失敗 Shard の retry、通過済み結果の保持 | A39〜A41 の統合テスト | adapter 本体 | S16・L3 |
| S19 | belt | GitHub status | 親だけが投稿、公開 commit と検査入力の一致検証、dirty / synthetic は success にしない、計算結果と投稿失敗を分けて報告 | A32 のテスト | local-ci 単体の status の変更（必要なら L 行を追加） | S16 |
| S20 | belt | toolchain | §22.1 の版指定の読み取り、baseline、match 規則、adapter（nodenv・rustup・mise・fvm・corepack）の detect / resolve / install / activate、setup と同じ経路の導入、`blocked: environment_mismatch` | A33・A42 のテスト | diff / sync（S21） | S10・S17 |
| S21 | belt | `workers diff` / `sync` | §22.2 の差分項目と 5 分類、sync は自動導入できる差だけ、`doctor` へ含める | A44 のテスト | 導入・常駐（S22） | S20・S22 |
| S22 | belt | 導入と常駐（macOS） | `workers add --install`（SSH bootstrap、版別ディレクトリ）、LaunchAgent / LaunchDaemon（`UserName`）、start / stop / drain / resume / `stop --cancel-running` | 1 台上での install → start → stop の統合テスト | upgrade・doctor（S23）・Mac 以外（S24） | S10 |
| S23 | belt | upgrade・rollback・doctor | drain → 配置 → 互換検査 → 切替 → health → resume、失敗時の rollback、doctor（task-traffic-controller runtime の互換・policy digest・state path・Docker の resource domain・service 環境での profile 検査） | A05・A34・A36 のテスト | Mac 以外 | S22 |
| S24 | belt | Linux の常駐と Colab の一時 worker | Linux の systemd user service（install / start / stop / upgrade）、Colab の一時 worker（`worker --ephemeral` をノートブックから起動、利用者が用意したトンネル経由で親が SSH、起動ごとに新しい identity、`workers forget` で消滅を宣言したときだけ interrupted に確定） | Linux（CI の ubuntu）での install → start → stop の統合テスト、ephemeral の identity と forget のテスト | macOS の常駐（S22）、トンネルの提供 | S22・S23 |
| S25 | belt | 1 台上の試験ハーネスと障害注入 | 複数 worker（別 state dir）を 1 台で立てる、kill 点・ACK 消失・時計のずれの注入口、A 対応表のうち実機不要のものを自動試験にまとめる | 実機不要の A がすべて自動で緑 | 実機の試験（S26） | S15・S16・S18・S23 |
| S26 | belt | Docker での受入と初回リリース | 手元の Mac と Docker の複数 Linux worker で A01・A02・A24・A26・A34・A35・A37・A45・A46 と、A51・A53・A54 の実機間でしか確かめられない部分（別マシンからの投入、実 SSH での鍵の配布、別マシンの停止）の適用可能な範囲を検証、結果の報告、0.1.0 の `cargo publish` とタグ・Release | 適用可能な A が緑、§20 の完了報告。独立 Mac 固有の未検証項目は明記する | 追加 Mac の調達 | 全行、Docker。受入環境は下記に従う |
| WP23 | belt | 直接実行と統一した exec | resident の有無に応じた exec、local worker 固定、共有台帳での admission | A50 |
| WP24a | belt | 仕様: グループと、run を投げたメンバー | `docs/spec.md`（仕様版 1.4、第24章と関連章）、`docs/plan.md`（この行と WP24b〜WP24e・R68・R96、A51〜A54・A56〜A58 の対応） | 節番号と A 番号の参照が合っている。公開禁止語の grep が 0 件 | コード | なし |
| WP24b | belt | グループのメンバーであることの確認と、投げたメンバーごとの所有 | hello の後の相互の `group_proof`（feature `group_membership_v1`、必須。hello の答えの nonce、proof の前に identity・epoch・能力を返さないこと、接続した側が worker の proof を確かめるまで仕事も secret も送らないこと）と断る理由（`group_mismatch`・`group_key_mismatch`・`member_identity_mismatch`・`identity_retired`）、メンバーの名前と `coordinator_id` の組の固定、グループ鍵の世代（番号と指紋、今の鍵と次の鍵）を持つ置き場（今の単一の `cluster_key_path` を世代 1 として引き継ぐ）、鍵の切替で接続を閉じること、次の鍵を持つ worker が断られた時点で自分で切り替えること、worker DB を `(coordinator_id, attempt_id)`・`(coordinator_id, task_id)` の鍵へ作り直すこと（drain 済みのときだけ。live な attempt があれば断る。古い行は捨てる）、`coordinator_id` ごとの `coordinator_epoch`・lease key・attempt の所有（他のメンバーの attempt は `NOT_FOUND`、同じ ID でも互いに `RETIRED`・`CONFLICT` を起こさない）、届かなかった attempt の tombstone の上限と消去、退役の通知 `identity_retired`（feature `identity_retired_v1`、必須。WP21 の残り。通知を受けた一時 worker は、どのメンバーの live な attempt も、退役を告げていないメンバーの未回収の result も無くなったら、自分で終了して state を消す）。自分で次の鍵へ切り替えたメンバーは、自分が張った接続も閉じて新しい鍵で張り直す。別のメンバーの attempt が持っている `attempt_id` への submit は、何も保存せずに `OVERLOADED`（`overloaded_reason.reason = "attempt_id_in_use"`）で断る（attempt の directory・supervisor・task-traffic-controller の ID が `attempt_id` だけで名前を持つため、1 つの worker が同じ ID を持てるのは 1 メンバーの分だけ。仕様 §24.4）。断られた側は task を失敗にせず、新しい `attempt_id` で配置し直す（`in-place` の task は自動で置き直さないので、同じ attempt を後で送り直す）。この行では、送信回数の記録（WP24c）がまだ無いので、常に retire を通してから置き直す（「送信の結果が不確かなとき」の規則。1 回だけ送って断られたときに retire を省くのは WP24c で容量の断りと一緒に入れる）。「何も行っていない」断り（`OVERLOADED`・`INVALID_INPUT`・live な attempt による `CONFLICT`）は、要求が運んだ新しい epoch も tombstone の消去も残さない。worker DB の step 4（live な attempt か未回収の result があれば断る）、coordinator DB の step 20 | 仕様 §24.2 の認証・§24.3・§24.4 の所有と epoch のテスト。A53 のうち鍵の確認（相互の proof、名前の固定）と退役の通知、A58（live な attempt があるときの拒否と、drain 済みの移行）。A18・A19 が変わらず通る | 鍵の配布と更新の command（WP24d）、調停（WP24c） | WP24a |
| WP24c | belt | 投げたメンバーをまたぐ調停と、断られた後の置き直し | worker が自分の設定の受付窓を全メンバーの合計で数えて断る（`overloaded_reason.reason = "capacity"`、feature `shared_admission_v1`、必須。断った worker は tombstone を含めて何も保存しない）、`inspect` の受付窓（上限・使用数・共有の有無）、投げた側が自分の値と worker の値の小さい方で数えること、forecast の `foreign_demand` と coverage の絞り込み、1 回だけ送って断られた attempt は retire なしで閉じて次の generation へ（理由 `capacity_refused` は自分の DB にだけ記録）、送信の結果が不確かな attempt は retire →未開始の確認→次の generation、一時的な除外と `task_waiting`（`capacity`）、配置理由と `estimate` の表示。coordinator DB の step 21（必要な場合） | 仕様 §24.4・§24.5 と A52 のテスト（2 つのメンバーの同時配置、断った worker に何も残らないこと、不確かな送信の後の遅れた submit が `RETIRED`、最大 1 回の開始、置き直す先が無いときの `task_waiting`、local worker が他のメンバーの仕事で埋まっているときの自分の run）。A06・A08・A19 が変わらず通る | メンバーごとの予約・比例配分（初回リリースに含めない） | WP24b |
| WP24d | belt | メンバーの設定と CLI、鍵の配布と更新、1 台が両方の役割を持つこと | `[[workers]]` をメンバーの一覧として読む（自動発見なし、`id` はメンバーの名前）、`workers add` の SSH helper がグループの ID・鍵・名前を渡すこと（同じグループならそのまま、別のグループなら断り、`--move` と live な仕事が無いことの両方があるときだけ移す）、`group`・`group rotate-key`（各メンバーの一覧を集めて和集合を確かめる→配る→切り替えるの順、`--remove`・`--skip`、途中からの再開、別の指紋の次の鍵があるときの拒否、`--remove` が SSH 鍵を消す手順を表示すること）・`group send-key`（forget したメンバーを戻すときは worker identity を作り直す）、外したメンバーへ SSH で到達できる間の `group`・`doctor` の警告、鍵の更新で外したメンバーへの `workers forget`（lease と停止猶予が過ぎるまで断る）、既に belt を持つ host への `workers add` が 2 つ目の worker を作らないこと、導入済みの host での `init` が同じ resident に coordinator の役割を足すこと、local worker と他のメンバーが使う worker が同じ agent・同じ worker DB であること、`workers stop` の拒否（`--all-submitters`）、`doctor` と `group` の `group_key_mismatch` 表示 | 仕様 §4「グループの管理」・§5・§15・§24.1・§24.2 のテスト。A51 と A53（鍵の更新・削除・取り残されたメンバー・途中で落とした後の再開）と A54 の `stop` の拒否のうち、1 host で確かめられる部分。鍵が引数・ログ・journal に出ないことのテスト | メンバーの更新（R68）、遅い peer の切り離し（R96）、メンバーの一覧の自動同期 | WP24b |
| WP24e | belt | 1 host 上の 2〜3 メンバーの受入 | S25 のハーネスで、別の state directory を持つ 2〜3 個のメンバーを 1 台に立てる。A51〜A54・A56〜A58 を自動試験にし、A 対応表と S25 の表へ試験名を書く。投げたメンバーの停止・答えないメンバー・鍵の更新の途中での停止の注入口 | A51〜A54・A56〜A58 が 1 host の自動試験で通る。実機間でしか確かめられない部分（別マシンの電源断、実 SSH での鍵の配布）は S26 の確認待ちとして区別して書く | 実機の受入（S26） | WP24c・WP24d・R68・R96 |
| R68 | belt | resident が自分を更新する（メンバーの更新） | `belt upgrade [--binary <path>]`: 更新のための保留（drain とは別の永続状態で、`resume` では解けない）→受理済みの仕事の完了待ち→親 DB と worker DB を同じ時点で退避 → 版別 directory へ配置 → 互換検査 → service 切替 → 実行した CLI が health を確かめて commit → 保留を解く。commit までは coordinator の役割を adopt と lease 更新だけに制限し（配置・generation の作成・投入の受理・retire と cancel の送信・回収の確定をしない）、worker は他のメンバーからの状態を変える要求のすべてに、何も保存せず理由なしの `OVERLOADED` を返す。commit の期限は service の入口（更新されない側）が見張り、過ぎたら新しい版が応答していなくても元へ戻す。rollback は 2 つの DB を一緒に戻す。`workers upgrade` が相手のマシンで同じ手順を起こす。process lock で同時に 1 つ | 仕様 §24.8 と A57 のテスト（run を抱えたメンバーの更新で attempt を失わない、失敗時に 2 つの DB が一緒に元の版へ戻る、rollback の後に user command を再び開始しない＝二重実行が無い、試用中に別のメンバーが出した retire が rollback の後に届いて二重実行にならない、新しい版が応答しないときに入口が期限で戻す、N と N+1 のメンバーが両方向で動く） | release の成果物の自動取得 | WP24b・WP24d |
| R96 | belt | 遅い・応答しないメンバーが CLI と他のメンバーを止めない | coordinator の tick が worker との通信を待つ間 actor を持ち続けない構造へ直す（worker ごとの独立した処理、actor へは結果だけ）。自分の DB だけで答えられる CLI 要求を他のメンバーの状態から切り離す。照会が要る CLI は答えた分だけ返す。受ける側で、あるメンバーの接続が他のメンバーへの応答を塞がないこと | 仕様 §24.7 と A56 のテスト。worker への呼び出しが永久に答えないままの状態で CLI 要求が完了することを構造として確かめ、加えて仮想の時計で 2 秒以内に返ることを測る、決定的なテスト（sleep に頼らない） | `worker_budget_ms` の既定値の変更 | なし（WP24b と並行可） |

正式名は決定済みである（2026-11-20）。名前は belt、crate は `belt-cli`、バイナリは `belt`、lib は `belt` とする。旧名 remote-task-runner（略称 rtr）からの改名は WP22 で実施した。以降の記述は新名に揃えてある。

## 現在の進捗と追加作業（2026-11-14）

この計画を進捗と追加変更の基準とする。完成に必要な不足が判明したら、理由・担当・検証条件・依存を追記する。実装済みの部品と、利用者が実際に使う経路への接続は区別し、接続と受入検証が終わるまで担当行を完了にしない。

- S0〜S14 と S17（PR #38、merge `9a9abc8`）は main に統合済み。S17 の検証対象 `279449b` はローカル実行 9 ジョブとリモート 9 ジョブが成功。S15 以降の接続と、追加検証で見つかった回復・テストの問題を別変更で進める。
- S15 は以下の小さい PR に分ける。S15a は S17 の統合結果へ載せ直し、PR #40 で本流へ統合済み。S15b の library 常駐基盤も実装・独立レビュー済みであり、別 process の bootstrap と CLI 接続は S15c で検証する。独立した修正・調査は担当を分け、設計と統合は親担当が行う。
- 検証はコミット済み HEAD に対する `local-ci run --include-disabled` を基準とし、対象 SHA、コードの失敗、環境エラー、未再現の範囲、リモート CI の結果を分ける。共有ツールが原因なら、そのツールで修正・検証・公開した後、belt の利用版を別変更で追従する。

| 追加行 | 担当 | 必要になった理由・スコープ | 完了条件 | 依存・状態 |
|---|---|---|---|---|
| F1 | TTC | 同じ秒に開始した無関係な session leader が、孤児の候補から外れない。同 boot の単調時刻で子より古いと証明できる場合だけ除外する | 修正前に失敗する決定的な回帰テスト。unknown・equal・newer・別 boot・矛盾する証拠は保持。関連統合テストと CI、修正版の公開 | PR #29 統合、0.8.1 公開済み。ローカル実行ジョブ 5 件・リモート 5 件成功。ローカルの installer action 未再現は別記 |
| F2 | fast-worktree | 並行する Git worktree 登録が、別の登録途中の空 `commondir` を読む競合 | Git ネイティブの登録操作と関連する読取・削除を repository 共通の排他で保護。hook は抑止せず、同 repo への同期再入は lock を迂回せず即時エラーにして deadlock を防ぐ。並行登録・hook の実行と拒否・再入・失敗時の状態の回帰テストと CI、制限を明記した修正版の公開 | PR #11 統合、0.4.0 公開済み。ローカル 3 件・リモート 3 件成功。native Git hook 中は同 repo の操作が直列化される。依存準備と fast-worktree の設定 hook は排他外。独自の排他を使わない raw Git は保護範囲外 |
| F3 | belt | 修正済み共有ツールと実行環境への追従 | TTC・fast-worktree の修正版を利用し、必要版・表示・テスト・文書を一致させる。local-ci の実行失敗を解消し、PR #38 をマージ | F1・F2 完了、PR #38 統合済み。追加の型付き回復 API は F8 で扱う |
| F4 | 検証環境 | CI の Rust 1.100 と古いローカル formatter・coverage ツールの差 | 同じ Rust 版と対応する coverage ツールで検証。新しい compiler で判明した未カバー分岐をテストし、ゲートを維持 | formatter と forecast の不足テストは PR #38 に反映。coverage ツールを CI と同版へ更新済み |
| F5 | belt | protocol flood のテストが無受信 1 秒を全 request 到着とみなし、遅い RPC を取り残す | 全 caller の応答と受理数を照合して終了。RPC が同期の各 phase より先に timeout しない階層予算を持ち、遅延到着を sleep なしで再現する回帰テストを追加 | 関連 3 テストを各 20 回、計 60 回成功。HEAD `279449b` の全体ローカル実行ジョブ成功。Overloaded/Internal の分類と件数の検査は維持 |
| F6 | belt CI | 同一ホストの mutation 4 shard がそれぞれ多数の Rust/Tokio integration test を並列起動し、基準実行の lease・deadline 検証を過負荷にする | mutation job に `RUST_TEST_THREADS=2` を設定し、4 shard・全テスト・全 mutant・判定基準は維持。cargo-schemata の基準実行と mutant 実行への継承を確認 | 既知失敗 5 件×4 プロセスと、関連 module 114 件×4 プロセス、計 456 件の module 検証が成功。HEAD `279449b` の全体ローカル実行 9 ジョブ成功。導入 action 未対応による incomplete は保持。リモート 9 ジョブも成功 |

| F7 | belt test | 稼働中 SQLite DB と SHM を同一 process で raw open/close すると POSIX lock が解除され、別 writer 終了時の WAL 消失と readonly エラー 522 を起こす | 独立 process で DB/WAL/SHM の物理バイトを検査し、論理削除済みデータの漏洩検査も維持する。readonly 接続を弱めず、最小再現と WAL 回帰で検証 | 通常 cleanup の関連 3 件と追加回帰 4 件が成功。独立 process の raw scan で論理削除済み WAL の物理残存も検査し、PR #38 は統合済み。VACUUM による圧縮 snapshot は検査範囲が狭くなるため採用しない |

| F8 | TTC・belt | 回復時の TTC `ProbeUnknown` を通常の失敗に数え、identity の不確定と他の失敗が混在した場合も分類できていなかった | 同じ ledger lock 内の解決判定を型付き API で返し、文字列解析・再 scan を避ける。ProbeUnknown だけの不確定は再確認し、混在原因は区別する。新 launch や偽 terminal を作らず回復する決定的テストと全 CI | TTC PR #30 の観測同期修正と PR #31 の API を統合し 0.8.2 公開済み。ローカル・リモート各 5 ジョブ成功、配布 8 asset の checksum 一致。belt の関連 unit 31 件・実回復 1 件・pure mutation 2 caught 成功、belt `623cfd5` のローカル実行 9 ジョブとリモート 9 ジョブ成功、PR #39 を merge `3270420` で統合済み。負荷下の既存時間依存テストは別件として残し、未証明の CPU guard 設定案は採用しない |

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

S15f の追加 slice（2026-11-17、main `4a6ec25` 起点）は、見積と配置の一致と配置理由の表示である。保存済み task の `estimate --task` が、同じ状態の配置と違う worker を答えることがあった。原因は 3 つである。(1) 配置は親の実測と cost model（`CostModel::costs`）の値を合成して比べるが、見積は実測の照会（`query`）だけを使っていた。(2) 配置は不明な部分へ最悪の既知値を入れてから forecast を照会するが、見積は照会のあとに入れていたので、worker へ渡す ready の遅れが違った。(3) error で答えた worker を、配置は数値の無い候補として比べ、見積は除外していた。

対応:

- 合成を pure な `scheduler::preview::merge_costs` に出し、配置（`place_one`）と見積の両方から呼ぶ。配置の挙動は変えていない（同じ式を移しただけ）。
- 見積を 2 段にした。1 段目で全 worker を `inspect` して候補と cost を決め、`fill_costs` のあと、2 段目で forecast を照会する。workload は配置と同じ `policy::workload_of_spec` で作る（CI task の step を含む）。forecast の年齢は全員の答えが揃った時点で測る。
- 見積でも、その task を断った worker（`worker_cannot_run`）と CI task を受け取れない worker を、配置と同じ理由で外す。どちらも読み取りだけである。
- 人向けの表示を pure な `scheduler::preview::human` に移し、選んだ worker と理由に加えて、規則の数値、worker ごとの priority と補正量・load・待ち・実行、転送・setup・回収それぞれの種別（実測は標本数と残り TTL、確認済み ready、cost model、順位付け専用の仮の値、unknown と理由）、除外した worker の理由と unmet を出す。仮の値と fallback の順位は時間として表示しない。`--json` の形は変えていない（古い owner の応答もそのまま読める）。
- `src/scheduler/preview.rs` を `scripts/pure-modules.txt` に追加した。

検証（手元、rustc 1.100.0）:

- `tests/it/scheduler.rs` `a_preview_names_the_worker_the_placement_then_takes_and_moves_nothing`: busy な worker と cold な worker で、cold の setup が長い場合と短い場合（勝者が入れ替わる）のそれぞれについて、queue にある task の見積を 100 回取り、queue・attempt 数・待機理由・run の event 数・各 worker の保持 attempt と submit／cancel／retire／renew の受信数が変わらないこと、続く配置の配置理由（worker・理由・順序・内訳の全フィールド）が 100 回の見積の `decision` と等しいことを確かめる。修正前はこのテストが失敗した（見積が cost model の値を持たず、cold を選んだ）。
- 既存の `coordinator::estimate::tests::estimate_100_calls_preserve_every_durable_row_epoch_and_actor_state`（DB 全行・epoch・lease seq 不変）と、準備 identity・resident・bootstrap の見積のテストはそのまま通る。
- `scheduler::preview` の unit test は出力の全文を比べる。

queue にある task の見積（2026-11-17 に決定）: `estimate --task` の対象が queue で配置を待っている task なら、見積は配置が実際に決めることを答える。配置の入力の条件を写した。

- source が ready な worker と、ready ではないが転送を実測済みの worker（`ready || measured_cold`）だけに forecast を照会する。どちらでもない worker は決定に載せず、前提の欄に「照会しなかった」と示す。
- run の入力を断った worker は `InputRefused` として理由付きで外す。
- attach 中のどの worker も source を持たず入力も断っていない間は、配置と同じく決定をしない。見積は `decision` を返さず、まだ決定が無いことと理由を前提の欄に示す。
- source を持たない worker は、使えて期限内の forecast の答えがあるときだけ比べる（配置と同じ pure な `forecast_counts` を両方から呼ぶ）。
- 外す理由を調べる順序を配置に合わせた（到達不能・drain 中、満杯、入力の拒否、source の有無、能力、task の拒否）。満杯で能力も合わない worker の理由は配置と同じ「満杯」になる。
- queue にあるかどうかは、配置が queue を読むのと同じ条件の読み取り（`estimate_queued`）で決める。保留中の CI task は queue に無いので、仮の追加実行として答える。
- gate の判定（`source_gate`・`waits_for_input`・`forecast_counts`）は pure な `scheduler::preview` に置いた。cold の worker が選ばれても prefetch（`prefer`）は起こさず、配置がまず source を送ることを前提の欄に示す。

検証: `tests/it/scheduler.rs` `a_preview_of_a_queued_task_follows_the_input_of_its_run_as_the_placement_does`。入力 gate を持つ worker 2 台で、(1) 誰も source を持たない間は見積が決定を返さず、tick も配置も待機理由の記録もしない、(2) source が片方だけで ready な場合と、もう片方が入力を断った場合のそれぞれで、見積（10 回）の `decision` が続く配置の配置理由と等しく、見積の前後で queue・attempt・event・worker の保持と受信が変わらない、ことを確かめる。実装前はこのテストが失敗した（見積が source を持たない worker を選んだ）。

決めたこと（spec 第 9 章「読み取り専用の見積」に記した）:

- queue に無い保存済み task（実行中・終了済み・取消済み・保留中の CI task）の見積は、仮の追加実行として答え、source の ready で候補を絞らない。配置はこれらの task を拾わないので、比べる相手の配置が無い。
- 古い epoch を理由に断られた forecast で、見積は fence しない。答えの無い worker として外す。
- 能力が合わない worker の `unmet` の文言は見積の方が詳しいことがある（frozen toolchain の照合結果など）。外す結論は同じ。
- 未投入の command の見積は従来どおり（入力の条件は使わない。cost model の task adapter は task が無いので呼ばない。error の答えは除外）。外す理由を調べる順序だけ、保存済み task と同じ順に揃えた。

S15f の残り（この変更では閉じていない。行は未完了のままとする）:

- setup の実測モデル: 観測を setup の秒数・ready の遅れ・warm の判定へ変換する接続（実 TTC の ledger identity と依存・resource graph の対応、artifact の完全検証と正確な activation の証明）。それまで setup は unknown。
- 実 worker・実 owner を通した利用者経路で、見積と配置の一致を確かめる受入（今回の証明は in-memory の worker と、実物の入力 gate を付けた in-memory の worker に対するもの）。Docker 受入。
- 変更後の pure coverage と全体の mutation は親の gate で確かめる（今回は `src/scheduler/preview.rs` と `src/scheduler/decide.rs` だけを `cargo schemata` にかけた）。

S15c の常駐入力は既存 user config の隣にある private `coordinator.json` に保存し、既存 bootstrap input を再利用する。`config.toml` の文法や暗黙の環境変数は追加しない。未初期化は明示エラーとし、`ls` / `show` は停止中でも読み取り専用 DB を使い、起動・epoch 更新・migration を行わない。`submit` だけが必要な owner 起動を要求する。

S15c の残件のうちメモリ上限の 3 点（下の段落の file/reader 転送と active attempt のページ走査に当たる分）を直した。(1) Coordinator の `live_attempts` 4 箇所は全件読みをやめた。`detach`・`detach_link`・tick の worker ごとの走査は `live_attempt_page` を 64 件ずつ、64 件に満たないページが返るまで読み（空のページまで読まない。cursor は安定しているので、短いページのあとに増えた行はどのみちこの走査の対象外）、1 ページずつ処理する。`send_placed` は attempt 1 件を探すだけなので、ページ化ではなく id と worker での 1 件読み（`live_attempt`）にした。(2) `unacked_demand` は worker ごとの `unacked_demand_page`（`placement_id` の keyset、64 件）に置き換えた。配置と estimate が持つのは forecast に載せる plan（worker ごとに最大 64 件）だけで、demand の JSON は持ち出さない。読めない placement を飛ばしてから 64 件を数える従来の規則は変えていない。(3) manifest の上限は worker 側の既定 64 MiB（`BlobLimits::max_manifest_bytes`）で、全体をメモリに持ってよい大きさではない。上限の強制だけで済ませず、compact JSON を source store の private な `tmp`（state と同じディスク）に作る名前の無い一時ファイルへ 64 KiB の buffer で書き出し、blob/bundle と同じ reader 転送で 64 KiB ずつ送る。wire の bytes と digest は従来と同じである。書き出しが 64 MiB を超えた manifest は送る前に失敗させる。保証は、走査の間ずっと対象である行はちょうど 1 回、走査中に増減した行は高々 1 回である。ページ境界での増減、0・64・65 件、3000 件で 1 回の読み取りが 64 件以下であることを storage のテストで、ページを超える件数の detach・tick・settle を Coordinator のテストで確かめた。全件を読む `live_attempts`・`unacked_demand` はテストと診断用に残し、Coordinator からは呼ばない。

この修正で残した限界: 送信側は manifest を parse した構造（`closure`）として持つため、メモリは manifest の大きさ（上限 64 MiB の JSON 相当）に比例する。parse の streaming 化は local-ci の `load_manifest` に及ぶので範囲外とした。ページ読みの時間は、coordinator の DB に足した部分 index（migration の最後の step）で履歴の長さに比例しない。`placement(worker_id, placement_id) WHERE acked_at_ms IS NULL` は未 ACK の placement を、`attempt(worker_id, created_at_ms) WHERE state <> 'terminal' AND retired=0` は生きている attempt を `(created_at_ms, rowid)` の順に、終了済みの行を読まず temp b-tree の整列もなしで返す（`EXPLAIN QUERY PLAN` をテストで確かめた）。index だけの step で feature marker は持たず、schema version の増加で旧 binary は拒否される。未 ACK の placement の走査は worker ごとに行うので、配置 1 件あたりの走査回数は 1 回から worker の数に増えた。送信側の 64 MiB の規則は spec 11 章に書き、値は受信側の既定と同じ `BlobLimits::default().max_manifest_bytes` を使う。

S15c の残件には、file/reader ベースのメモリ上限付き source 転送、active attempt のページ走査、requires/profile の配置条件、success/failure/always と glob・複数 path の出力計画を含める。現在の限定的な submit 経路でこれらを明示拒否していても、完成条件からは外さない。終了済み worker の spec・workspace は回収 ACK 前にも削除し、保持対象の結果・ログは残す。親側 source/Vault の retention は S15e で retry と回収条件を保護して接続する。

S15c は source/secret capture、CLI 終了後も動く local durable submit、bounded な offline query を実装・独立レビュー済み。blob/bundle は同じ検証済み FD から最大 64 KiB ずつ送り、転送停止時は owner を保持せず専用 lease で残る IO と cleanup を保護する。関連 7 件・strict clippy が成功。PR #42（merge `2b3078c`）で main に統合済み。

S15c 残件の確認（2026-11-17、main `a5823f8` 起点）。5 項目とも実装は main にあり、この変更は不足していた検証 2 件だけを足す。src の本体は変更していない。

- file/reader の source 転送: `service::forward` が検証済み FD を 64 KiB ずつ `put_blob_reader_as` へ渡す。根拠は `source_upload_opens_verified_descriptors_without_links_or_whole_reads` と forward の unit 6 件。manifest は 1 件ずつ JSON 全体を送る（blob/bundle ではなく metadata）。
- active attempt のページ走査: secret 配送は `live_attempt_page`（16 件、上限 64）と `live_attempt_subset` を使う。直接の検証が無かったため、70 件を 16 件ずつ欠落・重複なく走査し、terminal・retire 確定・別 worker を除き、上限 64 と subset 64 件超の拒否を確かめる storage テストを追加した。Coordinator 本体の `live_attempts`（4 箇所）と配置時の `unacked_demand` は、このテストの時点では全件読みだったが、この PR（ページ読みと部分 index）で `live_attempt_page`・`unacked_demand_page` に置き換えた（下の段落）。
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

- S26 の実機試験には Mac mini を使う（2026-11-20 に決定）。Mac mini が独立した Mac の sleep・boot・launchd・実機間転送などを担い、複数 Linux worker と Linux 検証には承認済みの Docker を使う。それ以外の追加の Mac/Linux 実機は準備しない。別 state dir と portool で分離し、複数 worker、親の再起動、接続断、ACK 消失、worker 消失、cold/warm と親単独との差を確認する。
- Docker 上の Linux worker と macOS ホストの結果は区別する。Docker を別の Mac 実機と記載せず、独立した Mac の sleep・boot・launchd・実機間転送など、代替できない項目は実機確認待ちとして A 対応表と最終報告へ残す。
- 公開する版には Docker で確認した範囲と未検証範囲を明記する。実機未確認の項目を、A01〜A49 全件確認済みと報告しない。

## A01〜A58 の対応

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
| A49 | S13（配置）・S3（設定）・S15a（設定から scheduler への接続） | |
| A50 | WP23（直接実行・共有台帳・local worker 配置） | direct_exec と bootstrap の A50 テスト |
| A51 | WP24d・WP24e | ○（別マシン間は S26） |
| A52 | WP24c・WP24b（所有と epoch）・WP24e | |
| A53 | WP24b（鍵の確認・退役の通知）・WP24d（鍵の更新・削除）・WP24e | ○（実 SSH での鍵の配布は S26） |
| A54 | WP24b（投げたメンバーごとの lease）・WP24d（`stop` の拒否）・WP24e | ○（別マシンの停止は S26） |
| A55 | WP25c（組み込みの fast-worktree・portool と単独 CLI の共存） | |
| A56 | R96・WP24e | |
| A57 | R68・WP24e | |
| A58 | WP24b | |

### S25: 1 台上の試験ハーネスと A01〜A49 の自動試験の対応（2026-11-17）

2026-11-17 の調査方法とハーネスの記録を以下に残す。上の表は担当行を示す。この表は、各 A を今どの自動試験が確かめているか、何が S26（Docker・実機）に残るか、どの A は担当行が未了で試験が無いかを示す。「自動試験」の列の試験は、試験名と冒頭の説明を仕様 §19 の期待結果と突き合わせて選んだ。期待結果の一部しか試験が無いと分かった A は「一部」にした。assert まで読んだのは `one_host` の全件と A16・A33・A49 の試験で、A05・A26・A42 は試験の説明とこの計画の該当節を読んだ。ほかの行は試験名と説明までで、S26 の受入で期待結果の句ごとに確かめ直す。試験名は代表で、同じ A を確かめる試験がほかにもある。

ハーネスは `tests/it/one_host.rs` にある。1 つの親（`Coordinator`）と、worker ごとに別の state directory・別の supervisor process・別の protocol 接続を持つ本物の agent を、1 つの試験 process の中に立てる。既存の fixture（`worker_agent::Fx`、`coordinator` の `spec`・`only`・`Lost` など）を使い、別の仕組みは作っていない。障害の注入口は 3 つで、どれも試験 crate の中にあり、library と release binary には注入用のコードを足していない。

| 注入口 | 内容 |
|---|---|
| `Faults::lose` / `Faults::offline` | 親と 1 worker の間で、指定した操作の request か応答を 1 回失う。または link を切ったままにする。親・agent の再起動で接続を作り直しても同じ設定が続く |
| `Faults::skew_wall` | worker の壁時計が親とずれている状態を作る。worker が返す時刻（receipt の時刻と `spawned_at_ms`）を、親が受け取る前にずらす |
| `Kill`（`Agent`・`Supervisor`・`Machine`）と親の drop | agent だけを止めて起動し直す、attempt の supervisor を kill する、worker の process をすべて止めて別の boot ID で起動する、親を落として次の epoch で開き直す |

時計について: 判断に使う単調時計（`platform::monotonic_ms`）を差し替える口は作っていない。呼び出しが 20 ファイル以上にあり、差し替えるには本体に試験用の経路を足す必要があるためである。単調時計の側は、時刻を引数で受ける純粋関数（`supervisor/lease.rs`、`results/clock_policy.rs`）の単体試験が担う。実際の sleep は実機に残る。

区分の意味:

- **自動**: 実機が無くても確かめられる範囲を、自動試験がすべて確かめている。
- **自動＋残り**: 1 台で確かめられる範囲は自動試験にある。残りは S26（Docker・実機）か、他リポジトリの試験が担う。
- **一部**: 自動試験は一部だけで、残りは担当行が未了、または試験が無い。
- **未実装**: 担当行が未了で、期待結果を確かめる試験が無い。
- **S26**: 実際のプロジェクトと複数 host が要り、ここでは試験しない。

2026-11-18 に統合候補 `d4d5f47` の A01〜A49 全行をテスト名と照合した。その後、最終の main `8f32159`（2026-11-20）で GitHub Actions が緑になり、実 CLI を必須にした全試験（`BELT_REQUIRE_FAST_WORKTREE=1 BELT_REQUIRE_TTC_CLI=1 cargo nextest run --no-fail-fast`）も 2513 件中 2513 件成功、失敗 0、skip 11 件（すべて `#[ignore]`）だった。「統合検証済み」は、この main で上記の結果を確認した項目。各行の個別検証と S26 の実環境検証は別に示す。

| A | 区分 | 自動試験（代表） | 残り・検証範囲 |
|---|---|---|---|
| A01 | 自動＋残り | `scheduler::a01_tasks_given_to_no_host_run_on_the_parent_and_on_both_children`、`one_host::a01_three_real_workers_on_one_machine_each_run_one_of_three_tasks` | 別 host の worker（S26 の Docker） |
| A02 | 自動＋残り | `scheduler::a02_a_worker_that_joins_takes_the_backlog_the_parent_kept`、`one_host::a02_a_real_worker_that_joins_later_starts_the_backlog_the_parent_kept` | 別 host の worker（S26 の Docker） |
| A03 | 自動 | `scheduler::a03_a_busy_worker_beats_a_cold_one_and_the_reason_shows_queue_setup_and_transfer`、`scheduler::a03_a_cold_worker_that_is_set_up_quickly_beats_the_queue` | |
| A04 | 自動 | `scheduler::a04_submits_from_many_sides_are_decided_one_at_a_time`、`scheduler::a04_a_placement_no_worker_acknowledged_is_counted_by_the_next_decision` | |
| A05 | 自動 | `doctor::an_unpatched_task_traffic_controller_fails_doctor`、`supervisor::a_real_nested_cli_runs_under_its_step_and_shares_the_ledger`（scope の統一後。S23 doctor の節の「既定では成り立っていない」はその前の記述） | |
| A06 | 自動 | `forecast::a06_a_hundred_forecasts_add_no_entry_lease_job_or_reservation` | |
| A07 | 自動 | `a07_ready_delay_queue_and_runtime_make_the_finish`（`src/forecast/mod.rs`） | |
| A08 | 自動 | `forecast::a08_a_node_is_one_demand_before_it_registers_after_and_once_finished` | |
| A09 | 自動 | `a09_the_three_cases_stay_distinct` ほか 3 件（`src/forecast/mod.rs`） | |
| A10 | 統合検証済み | `forecast::tests::a10_matrix_variants_forecast_the_final_step_after_preparation`、`a10_parallel_nodes_end_with_the_longest_not_the_sum` | `8662486` の対象試験は6件成功。実測履歴と未計測時の予測を確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A11 | 自動 | `source_capture::a11_every_kind_of_change_is_captured_and_unpacks_to_the_same_content` | |
| A12 | 自動 | `source_capture::a12_an_input_that_keeps_moving_is_source_busy_and_nothing_is_published` | |
| A13 | 自動 | `source_capture::a13_a_capture_cut_off_before_publication_leaves_nothing_visible_and_resumes_as_the_same_id` | |
| A14 | 自動 | `source_capture::a14_unpushed_commits_travel_in_a_bundle_and_a_worker_never_fetches_origin`、`a14_a_dirty_submodule_is_captured_whole_and_travels_with_its_parent`、`a14_an_lfs_file_is_captured_as_its_payload_and_the_pointer_is_not_what_travels` | |
| A15 | 自動 | `setup::each_worker_builds_its_own_template` | |
| A16 | 自動 | `the_key_changes_with_everything_that_decides_what_is_made`（`src/setup/policy.rs`） | |
| A17 | 自動 | `setup::building_a_plus_b_leaves_template_a_alone_and_one_cancel_breaks_no_other` | |
| A18 | 自動 | `worker_agent::a18_an_attempt_resent_100_times_starts_its_command_once` | |
| A19 | 自動 | `coordinator::a19_the_task_moves_only_after_the_retire_and_the_late_submit_is_refused`、`worker_agent::a19_a_submit_that_arrives_after_the_retire_is_refused`、`one_host::a19_a21_a_lost_ack_and_a_gone_link_never_start_the_task_on_the_free_worker` | |
| A20 | 自動 | `supervisor::after_a_supervisor_crash_the_same_process_is_stopped_and_never_respawned`、`supervisor::a_launch_without_an_identity_is_investigated_not_respawned`、`supervisor::a_crash_between_the_termination_and_the_result_is_completed_by_the_next_start`、`one_host::a20_a_killed_supervisor_ends_the_attempt_once_and_nothing_is_started_again` | |
| A21 | 自動 | `supervisor::the_supervisor_finishes_the_attempt_after_its_agent_is_killed`、`coordinator::a21_an_attempt_whose_link_is_gone_is_kept_and_followed_again_on_return`、`one_host::a19_a21_a_lost_ack_and_a_gone_link_never_start_the_task_on_the_free_worker` | |
| A22 | 自動 | `supervisor::lease_expiry_stops_the_command_and_a_late_renewal_cannot_revive_it`、`coordinator::a22_after_a_long_stop_the_parent_keeps_unknown_until_the_worker_says_it_stopped` | |
| A23 | 自動 | `worker_agent::a23_a_restarted_agent_adopts_the_living_supervisor`、`coordinator::a23_a_restarted_parent_adopts_what_runs_and_starts_nothing_again`、`one_host::a23_a_killed_parent_is_followed_by_one_that_adopts_what_two_real_workers_run`、`one_host::a23_a_restarted_agent_of_one_worker_keeps_its_command_and_the_parent_follows_it` | |
| A24 | 自動＋残り | 擬似（process を止めて boot ID を変える）: `worker_agent::a24_a_changed_boot_id_interrupts_everything_and_a_retry_runs`、`one_host::a24_a_restart_of_one_workers_machine_interrupts_its_attempt_and_spares_the_other` | 本物の reboot と OS の boot ID（S26、Mac は実機待ち） |
| A25 | 自動 | `supervisor::cancel_terms_then_kills_the_group_and_records_only_after_it_is_gone`、`supervisor::natural_end_and_cancel_racing_still_give_one_terminal` | |
| A26 | 自動＋残り | lease は単調時計の値だけを受ける純粋関数で、長い suspend に当たる飛びを試験する: `a_long_suspend_expires_the_lease`・`a_late_renewal_does_not_revive_an_expired_lease`（`src/supervisor/lease.rs`）。壁時計の前後への飛びは保持期間の判定で試験する: `forward_jump_requarantines_then_follows_the_new_wall`・`backward_step_holds_briefly_or_requarantines`（`src/results/clock_policy.rs`）。worker 間の壁時計のずれ: `one_host::a26_workers_whose_wall_clocks_are_off_keep_their_leases_and_end_normally` | 実行中に OS の壁時計を変える試験と、実際の Mac sleep からの復帰は無い（実機待ち） |
| A27 | 統合検証済み | `bootstrap::a27_public_ci_expands_from_a_needs_output_and_joins_in_variant_order` | 対象試験3件成功。実 CLI の動的展開と join、複数 worker での permit・fail-fast を確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A28 | 自動 | `bootstrap::ci_plan_shows_why_a_shared_workspace_task_is_not_split` | |
| A29 | 自動 | `non_utf8_bytes_round_trip_per_stream`・`reads_resume_from_any_offset_without_duplicates`・`limit_is_shared_and_the_rest_is_counted_as_dropped`（`src/supervisor/tests.rs`）、`collection::active_raw_logs_are_durable_before_terminal_and_reconnect_at_committed_offset` | |
| A30 | 自動 | `collection::a30_a_worker_that_disappears_while_collecting_is_continued_where_it_stopped`、`coordinator::a30_a_real_workers_outputs_are_collected_over_two_connections_and_the_worker_is_told` | |
| A31 | 自動 | `collection::a31_an_empty_or_short_set_of_results_is_told_apart_and_never_a_success` | |
| A32 | 統合検証済み | `bootstrap::a32_ci_posts_a_status_only_for_what_the_commit_holds_and_a_failed_post_changes_no_result`、`ci::status::a32_a_dirty_or_synthetic_snapshot_is_not_posted_to_the_original_head` | 実 CLI が未コミット変更を含む実行で success を計算し、元 HEAD を記録しつつ GitHub 投稿を呼ばないことを確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A33 | 自動 | `worker_agent::capabilities_keep_optional_probe_unknown_but_required_tools_fail_closed`、`worker_agent::a_secret_that_cannot_be_masked_keeps_the_command_from_starting`、`secrets::a_run_that_lost_its_bundle_is_blocked_not_resumed_with_something_else` | |
| A34 | 自動＋残り | 偽の Launcher・偽の ssh: `upgrade_switches_checks_the_bridge_and_commits`・`an_interrupted_switch_is_finished_by_running_upgrade_again`（`src/service/workers/upgrade/tests.rs`） | 本物の launchd・実 SSH での install / upgrade / rollback（S26） |
| A35 | 統合検証済み＋S26 | `worker_service::tests::a_system_service_installs_stops_restores_and_upgrades_without_replacing_the_admin_file`、`doctor::tests::the_retained_service_environment_is_the_one_that_is_checked`、`systemd::tests::a_start_runs_reload_enable_start_in_order` | 偽の launchd・systemd と一時ディレクトリの管理者定義で復旧・起動順を確認し、doctor が記録済みの service 環境を使うことを試験する。実サービスのログイン・起動時の復帰と、その環境での doctor は S26。統合検証済み（main `8f32159`、2026-11-20）。 |
| A36 | 自動＋残り | `doctor::an_updated_task_traffic_controller_passes_the_runtime_check`（旧い形の `status --json` を読む） | 旧コマンドと主要 workflow の互換は local-ci・task-traffic-controller 側の CI が担う（L1a〜L3・T1） |
| A37 | S26 | | 実際のプロジェクトの CI を親のみと複数 worker で比べる |
| A38 | 統合検証済み | `remote_links::a38_public_ci_delivers_needs_outputs_and_a_verified_multifile_artifact_to_another_worker`、`collection::a38_live_artifact_transfer_resumes_before_either_worker_finishes` | 対象試験4件成功。実 CLI の 2 worker 間転送、ACK 再送と親再起動を確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A39 | 統合検証済み | `collection::a39_discovery_collection_expands_the_run_once_before_any_test_is_placed`、`a39_file_shards_cover_every_test_once_and_add_up_to_one_job`、`worker_discovery_lists_real_cargo_tests_then_runs_only_each_shards_assigned_test` | Jest・Vitest の全件と欠落を集約で区別する。実 Cargo では列挙で本体を実行せず、分割後は割当テストだけが実行され、その合否を receipt に残す。統合検証済み（main `8f32159`、2026-11-20）。 |
| A40 | 統合検証済み | `collection::a40_history_shortens_the_longest_shard_and_no_history_is_a_stable_split_that_runs`、`matches_identical_tests_and_scales_each_observation_once`、`discovery_precedes_the_matrix_journal_and_its_shard_timings_survive_replay` | 予測最長時間の短縮、決定的 fallback、同じテストの観測に基づく worker 速度補正、journal 再生後の履歴保持を確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A41 | 統合検証済み | `collection::a41_a_job_that_cannot_be_split_says_why_and_runs_as_one_job`、`a41_fail_fast_stops_the_running_shards_once_and_the_job_fails_with_a_report`、`a41_retry_rebinds_failed_downstream_needs_after_the_shards_rejoin` | 分割不可の理由、実行中 shard の停止、失敗報告、再実行後の依存結果を確認。統合検証済み（main `8f32159`、2026-11-20）。 |
| A42 | 自動＋残り | `bootstrap::frozen_toolchain_public_exec_installs_once_activates_setup_isolates_and_retries`、`bootstrap::frozen_toolchain_public_exec_blocks_what_cannot_be_installed_before_running`、`toolchain_preparation::all_five_adapters_execute_under_managed_supervisor_and_reuse_verified_receipts`、`worker_agent::a_forecast_counts_the_toolchain_installs_an_accepted_attempt_waits_for` | 導入待ちの負荷を含む S20 は PR #81 の実 Linux・macOS CI で成功。実 manager と network での導入は S26。 |
| A43 | 自動 | `secrets::a43_the_secret_files_reach_the_worker_as_0600_and_are_gone_after_the_attempt` | |
| A44 | 自動＋残り | 偽の ssh と偽の tool: `diff_classifies_the_five_ways_a_worker_can_differ`・`sync_blocks_what_needs_a_person_with_the_steps`（`tests/it/workers_lifecycle.rs` の `a44`）、`bootstrap::workers_sync_installs_through_the_resident_then_measures_again_and_needs_the_resident` | 実際の worker との差（S26） |
| A45 | 統合検証済み＋S26 | `systemd::policy::tests::a_system_user_unit_names_a_non_root_user`、`worker_service::tests::a_system_service_installs_stops_restores_and_upgrades_without_replacing_the_admin_file`、`src/platform` の単調時計・boot ID・process identity・sleep 抑止の試験、`tests/it/supervisor.rs` | 非 root の User= と登録・起動・停止の手順を一時定義と偽 launcher で検証する。実 Linux x86_64・arm64 への導入、実 systemd user service の起動・停止、system-user の実行ユーザー確認は S26。統合検証済み（main `8f32159`、2026-11-20）。 |
| A46 | 統合検証済み＋S26 | `two_starts_never_share_an_identity`、`remote_links::a46_a_tunnel_disconnect_keeps_the_ephemeral_attempt_until_explicit_forget` | 起動ごとの identity を検査。ローカルの SSH 代役と実 worker で、切断後の unknown 保持、明示 forget による interrupted と identity 退役、同じ worker が再接続して旧 identity が拒否されることを確認する。attempt 数も command 実行回数も 1。実 SSH トンネルは S26。統合検証済み（main `8f32159`、2026-11-20）。 |
| A47 | 自動 | `secrets::worktreeinclude_picks_only_ignored_untracked_files_and_the_store_never_holds_them`、`worker_agent::the_real_host_knows_a_worktreeinclude_file_and_a_placed_secret_under_any_name` | |
| A48 | 統合検証済み | `workspace::add_and_remove_a_commit_workspace`、`workspace::the_original_is_never_written`、`workspace::removing_an_attempt_releases_only_its_portool_allocation`、`secrets::worktreeinclude_picks_only_ignored_untracked_files_and_the_store_never_holds_them` | `00cd900` で未コミットの .fast-worktree.toml を含む snapshot の削除 hook を復元。実 fast-worktree と portool で同じ worker に 2 attempt を作り、一方を削除するとその workspace と割当だけが消え、最後の削除で割当が空になることを検査する。統合検証済み（main `8f32159`、2026-11-20）。 |
| A49 | 自動 | `scheduler::a_preferred_worker_is_taken_while_the_others_are_not_faster_by_the_correction`、`collection_is_added_to_the_forecast_and_the_priority_correction_to_that`（`src/scheduler/decide.rs`）、設定から scheduler まで: `runtime::runtime_attach_applies_only_verified_identity_priorities` | |

2026-11-17 の調査時点では、自動 29、自動＋残り 7、一部 6、未実装 6、S26 のみ 1 件だった。上表の「統合検証済み」は、その後に試験が統合され、main `8f32159` の GitHub Actions と実 CLI 必須の全試験（2026-11-20）で確認した項目を示す。

S25 の完了条件は「実機不要の A がすべて自動で緑」。main `8f32159` で GitHub Actions が緑、かつ実 CLI 必須の全試験が 2513 件中 2513 件成功（2026-11-20）したため、S25 は完了とする。A35・A45・A46 の「＋S26」と、各行の「残り」にある実機・Docker の検証は S26 が担う。

2026-11-17 のハーネス作成時の残件（当時の記録。2 の現状は上表 A48）:

1. retire の応答消失には、coordinator の試験でも `one_host` の `Faults` を使う。
2. A48 は同じ worker の 2 つの attempt に portool の資源を割り当て、片方の終了後にもう片方の割当が残ることを検証する。
3. 単調時計を試験から進める口は無い（上の「時計について」）。lease の失効を agent・supervisor の実 process で確かめる試験は、実時間の短い lease を使う既存の試験のままである。
4. kill の注入は、worker が launch と process identity を記録した後に限った（`OneHost::launched` で待つ）。spawn の直後で identity を記録する前に supervisor を kill すると、worker は仕様どおり起動不明として未解決のまま保持し、その command は同じ user のほかの attempt の回復からも「隠れた process」に見えて、同じ試験 process のほかの試験の回復を止める（最初の版で 6 回中 1 回起きた）。記録前の kill は `tests/it/supervisor.rs` の `a_launch_without_an_identity_is_investigated_not_respawned` が担い、ここでは注入していない。記録前の地点で確実に止める口は本体に試験用の経路が要るため、足していない。

## 確定事項（仕様 v1.2 に反映済み）

- Linux（x86_64・arm64）を正式対応する（systemd user service）。Google Colab は常駐なしの一時 worker とする。OS 依存部分（suspend を含む単調時計、boot ID、process identity、sleep 抑止）は platform module に macOS と Linux の実装を置き、S9a・S10 から両方で動かす。
- `.worktreeinclude` に一致する ignored ファイルは、自動で secret_files と同じ経路で送る。`secret_files` には追加分だけを書く。
- 追加実機が未準備のため、当面は Docker の複数 worker と Linux・障害注入で受入を進める。Mac 実機固有の未検証項目は対応表に残す。

### S15 public init integration

Mac user init wires one LaunchAgent to the foreground combined coordinator/local-worker resident, using private retained input, a receipt journal, hash-addressed binary and authenticated connect-only readiness. Initialization never starts a detached fallback. Journaled pending publication binds device/inode/digest before publication; foreign or uncertain state requires reconciliation. Ordinary init retains its pinned binary and requires the separate upgrade flow for changed bytes. Native launchd acceptance is still required: fake transport and owned-child tests do not establish real launchd behavior. Linux public init uses the systemd user backend since S24 (the journal records which service manager it was written for; see "S24 親の側"); real systemd acceptance is still required (S26). System-user installation, runtime WorkerControl stop/drain/resume, SSH remote enrollment and upgrade/rollback remain required S22/S24 dependencies, not completion exclusions.

Managed init binds planned cluster/coordinator IDs before preparing a staged metadata database at epoch 0; the first daemon claim advances it to epoch 1. A private optional managed-service reference in the immutable input makes normal bootstrap connect-only, validates journal/input/key/database receipts and refuses incomplete or foreign bindings. Readiness compares authenticated resident PID/start/boot identity with fresh running launchd observations. Stage parent directories and newly created directory ancestry are synced before a pending journal may reference them; uncertain journal/publication errors retain the exact staged inode for reconciliation. Legacy unmanaged bootstrap keeps its prior behavior.


同時 init が `init.lock` 作成前の directory 作成で競合し、`AlreadyExists` をエラーにする問題を追加検証で確認した。競合した entry は非 symlink の実 directory と再確認し、勝った側だけでなく競合した側でも directory と親を fsync する。private leaf の所有 UID・permission 検査は維持する。directory/file/symlink と fsync の決定的 unit、および同じ epoch・worker・cluster と 1 回だけの daemon 起動を確認する所有子 process の同時 init テストが成功。負荷下の後者は約 120 秒を要した。strict all-target clippy・fmt・diff 検査も成功し、最終 immutable HEAD の全体 CI は親担当で行う。

Standalone worker foreground slice: `worker --input` reads an owner-only retained
WorkerSpec (schema 1, enrolled cluster/worker IDs, pinned executable, private key,
allowlisted environment and captured user TOML). Its state must be the canonical
HOME worker state so `worker --stdio` reaches the same agent socket. This is a
persistent role; the worker side of `--ephemeral` is a separate role (see "S24 Linux の systemd user service と ephemeral worker").
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
controls and native platform acceptance remain required. The parent's side of
the S24 ephemeral identity and forget semantics is described under "S24 親の側".
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
belt PR44 の timing fixture 補正はローカル9項目・リモート9項目成功後にマージした。
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

- bridge の起動: install は worker の設定 dir に版に依らない `bin/current` symlink を作り、その絶対パス（`<config>/bin/current/belt`）を応答で返す。親は `[[workers]]` の `binary` に保存し、resident の bridge と lifecycle の helper はそれを使う（`binary` が無い旧登録は従来どおり PATH の名前）。`workers add --install` の再実行で旧登録にも追記する。install の最後に、resident と同じ経路（絶対パスの `worker --stdio` を ssh で起動し hello）を通し、worker_id と cluster の一致を確かめてから登録する。
- 動いている resident への反映: resident の認証済み control に `ReloadWorkers` を足し、`workers add` の最後に要求する。resident が無ければ何もせず、失敗は結果の `resident` に「再起動まで未接続」として出す。
- staging と probe は SHA-256 を照合し、部分転送を公開しない。再実行は worker.json の揮発する env を比較から外し、receipt の無い未完了 install は置き換えを許す。local worker 名の衝突は install 前に拒否する。lifecycle は応答の cluster を照合し、start・stop・drain・resume は実状態に合わせて報告する（loaded でなければ start は失敗、停止を確認できなければ stop は失敗）。start は cancel 指示を起動前に消す。状態 dir を読めない場合は 0 件と見なさずエラーにする。
- agent は drain 中の新規受理を `Overloaded` で拒否する（再送への応答は維持）。stop は 0 件を 2 回続けて確認してから bootout する。
- 親の OS 判定は注入でき、試験は全 OS で同じに走る。

S22 の残り（必須。この変更で S22 は完了しない）:
1. 本物の launchd・実 SSH・実機での受入（A34/A35 の native 検証、S26）は未実施。偽の Launcher と偽の ssh は実挙動を確かめない。
2. drain の確認と受理の間に、非常に短い競合が残る（受理が drain の確認を通った直後に stop が 0 件を 2 回数える）。実機での stop と受理の同時試験は S26 で行う。
3. `system-user`（LaunchDaemon の `UserName`）の導入、fish / csh のログインシェル対応は未実装。Linux・`workers forget` は S24、upgrade・rollback・doctor は S23 で入った。
4. 実 SQLite の live attempt を持つ worker に対する stop の待ち・cancel の end-to-end（agent の cancel は統合試験済みだが、helper の待ちは注入した計数での単体試験）。

### S21 `workers diff`（2026-11-17、1 本目: diff と待機理由の手順）

`workers diff [<worker>]` を実装した。親と worker は同じ測定関数（`toolchain::envreport::collect`）で測り、worker 側は登録済みの binary の隠し action `worker-service env` が ssh 越しに JSON の環境 report を返す。測定は resident と同じ永続入力（worker.json の env と profile の PATH）を使い、ssh セッションの PATH は使わない。report は tool ごとの観測（available / unavailable / unknown）と版、版管理 adapter（registry）の導入候補と導入済みの版、belt の build 版、`docker info` による実行可否、空き disk、profile の名前・required_tools・digest、policy digest を持つ。

task-traffic-controller と local-ci は belt の binary に組み込まれている（spec 22.4）ので、外部の `--version` は叩かず、双方の binary の build 版（`belt` の版）で比べる。portool・fast-worktree は他の tool と同じ 3 状態（ある・無い・測れない）で測り、測れないものを same にも missing にもしない。`docker info` は profile の HOME と worker env の `DOCKER_HOST`・`DOCKER_CONTEXT` を付けて呼び、エラーは Unknown とする（docker の実体が無いときだけ not runnable）。子は自分の process group に置き、出力は上限（8 KiB）を超えた分を読み捨てて pipe を詰まらせず、時間切れでは group ごと止める（`platform::signal_process_group`）。

差の判定は純粋関数 `toolchain::envdiff::diff(parent, worker) -> Vec<Item>`（`doctor` から呼べるよう公開、PURE 対象）。分類は `same / differs / missing / auto_installable / manual_required / not_measured` で、優先順位は次のとおり。unknown は同じと見なさず（`differs`）、手順は「測れなかった（could not be measured）」と言い、install は案内しない。manual の tool（xcode・docker）は版 manager が候補を持っていても常に `manual_required`。portool・fast-worktree は配布経路（版 manager の導入候補）が無ければ `manual_required`。worker の版 manager に導入済みの版は、既定の版が違っても、PATH に tool が無くても `same`（H1）。導入候補にある版が未導入なら `auto_installable`、候補外なら `missing` か `differs`。profile は名前と required_tools で比べ、digest は host ごとの絶対 path を含むため表示だけにする。policy digest は本物の digest ができるまで `not_measured`（same にしない。`all_same` の判定には入れない）。手順（steps）は項目ごとに付ける。古い binary・新しい schema・壊れた応答・ログインの banner・worker 側の拒否は、それぞれ別の文言の `CONFLICT` で、空の差にはしない。worker から来た文字列は制御文字を除いて出す。

`diff` を worker 省略で呼ぶと、1 台が測れなくても残りを続け、worker ごとに結果（測れなかった理由を含む）を出す。測れない worker があれば終了コードは非 0。登録が 0 台なら、その旨を出して非 0 で終わる。

適合 worker が無い submit の待機理由（`unmet`）には、未充足の tool 要求の直後に `how: …`（128 文字以内、文字数で切る）の 1 行を足す（`envdiff::with_steps`、`task_waiting` と `estimate` の両方）。manual の tool は固定の手順、それ以外は「worker の profile PATH に導入して」、観測が unknown なら「測れなかった」と言う。unmet の行の照合は `capability::unmet_line`（`missing` が行を書く関数）で行う。

A44 の diff 側: `src/toolchain/envdiff.rs` の単体試験、偽の ssh と一時 HOME の `src/service/workers/tests.rs`、実 binary・偽 ssh・偽の tool shim の `tests/it/workers_lifecycle.rs`（`a44`）。ネットワークには出ない。

残り（S21 は完了していない）: `workers sync` の実際の導入は 3 本目で入れた（下の節）。`doctor` への組み込みは 7 本目で入れた（下の「S21 `doctor` への接続」）。`diff` の worker 省略時の並列化、実機での受入（S26）。`installed_inventory` の所要時間（1 版ごとに detect）は未計測（L7）。task-traffic-controller と local-ci の比較は belt の版だけで、組み込んだ crate 個別の build 情報は持たない。

### S21 `workers sync`（2026-11-17、2 本目: 計画と blocked、導入の差し込み口）

`workers sync <worker> [--project <dir>]` を実装した。計画は純粋関数 `toolchain::envsync::plan(parent, worker, project)`（PURE 対象）で、diff の `auto_installable` を版管理 adapter の導入手順にし、それ以外（`manual_required`・`missing`・`differs`。platform・disk・profile を含む）は導入せず `blocked: environment_mismatch` として不足内容と手順を出し、非 0 で終わる。「in sync」と言うのは `diff` が `all_same` のときだけ（`--project` で宣言された tool が親と違うだけなら「プロジェクトの要求を満たす」と言い、`in_sync: false`）。`--project` は dir 直下の版指定ファイル（`.fvm` を含め途中の symlink も辿らない（`platform::open_beneath`）、各 256KiB 以下、1 つも無ければ InvalidInput）を `declarations` で読み、宣言された tool は親ではなくプロジェクトの要求（導入済みか既定の版が満たせば何もしない、満たす最大の許可済み版を導入、無ければ blocked）に従う。alias（`lts` など）・式は版ではないので導入せず、「具体的な版を指定する」と案内する。導入済みの版は PATH に tool が無くても要求を満たす。

実行は自動導入を先に行い、その後で手動分を blocked として報告する。導入後に worker を測り直し（比較は純粋関数 `envsync::verify`、メッセージは `envsync::render_blocked`）、導入が見えなければ blocked とする。途中で止まったとき（測り直しの失敗、残りの blocked）も、それまでに導入したものをメッセージに含める。導入の失敗は成功にせず、別の版へ切り替えない。導入そのものは `Installer` trait の差し込み口に分けた（本番の実装は 3 本目の `ResidentWarmup`。2 本目の時点の `NoWarmup` は削除した）。

残り（S21 は完了していない）:
1. 導入の本番経路は 3 本目で入れた（下の節）。
2. `doctor` への組み込みは 7 本目で入れた（下の「S21 `doctor` への接続」）。
3. `--project` は workflow の `setup-*` 入力を読まない（版指定ファイルのみ）。（2026-11-23 追記: 読むようになった。済み。）

### S21 `workers sync` の導入（2026-11-17、3 本目: warm-up の要求）

`workers sync` が worker に実際に導入するようにした。経路は spec 11 章が名前を挙げている `env_sync` で、wire の型は以前からあり、agent が未実装（`NOT_IMPLEMENTED`）だったものを実装した。

- **経路**: `workers sync` → 動いている resident の control 操作 `env_sync`（`Operation::EnvSync`） → `Coordinator::env_sync` → 既に張ってある認証済みの worker link → wire の `env_sync` → worker の agent。ssh で worker のファイルを読み戻すことはしない。admission の lock も SQLite の書き込みも持たない（`resolve_toolchains` と同じ扱い）。
- **agent の導入**: 親が渡すのは tool 名と厳密な版だけで、導入の内容はすべて worker 自身の registry（`toolchains.toml`）から作る（`Registry::inventory_request`。recipe が無い tool、inventory に無い版、厳密でない版は導入せず、理由を付けて `failed`）。導入は `Host::toolchain_poll` に warm-up 用の consumer を渡して行うので、配置済みの attempt と同じ setup の supervisor・forecast・task-traffic-controller の gate を通る。install plan の key は project や親の profile に依らないため、同じ版の warm-up どうしも、その版を要する attempt も、既存の singleflight で 1 つの導入にまとまる。
- **待たない要求**: link と control socket の応答時間は数秒なので、1 回の要求は導入を待たず `pending` を返す（`SyncOutcome::Pending` を追加）。親は同じ `sync_id`（要求の本文に追加。`request_id` は毎回新しい）で聞き直す。agent は `sync_id` ごとに consumer を 1 つ持ち、決着（導入済み・失敗・`prepare_timeout_ms` 超過）で参照を手放す。決着した結果は覚えておき、応答を失った親の聞き直しに同じ答えを返す（失敗した導入をやり直さない）。誰も聞きに来ない warm-up（親が途中で止まった）は、agent の tick が `warmup_idle_ms`（既定 60 秒）で参照を手放す（最後の参照なら導入も止まる）。同時に覚える warm-up は 16 個まで、1 要求の tool は 32 個まで（超える要求は丸ごと断る）。停止中の agent は新しい warm-up を始めない。古い coordinator epoch は `STALE_EPOCH`。`dry_run` は検出だけで何も始めない。
- **旧 peer の除外**: hello の feature `env_sync_v1` を足した。双方が提供し、どちらも要求しない（`required_features` に入れない）ので、片方だけが新しい組み合わせでも接続と通常の配置はそのまま動く（N/N+1）。合意できなかった link には coordinator が要求を送らず、`workers upgrade <worker>` を案内する `PROTOCOL_INCOMPATIBLE` を返す。agent 側も、合意していない接続からの `env_sync` を agent に渡す前に断る。古い resident（操作を知らない）には「resident を再起動して」と案内する。
- **CLI**: `NoWarmup` を `ResidentWarmup` に置き換えた。導入する版が 1 つでもあるときだけ resident に繋ぐ（揃っている worker の sync に resident は要らない）。resident が動いていなければ、起動せずに「resident が動いていない」と出して止まる。応答なし・混雑（`COMMUNICATION_UNKNOWN`・`OVERLOADED`）は同じ `sync_id` で聞き直すが、誰も答えない状態が 2 分続いたら打ち切る（link が切れた worker、この操作を知らない古い resident は要求を読めずに接続を閉じるので、ここに当たる。メッセージで resident の再起動を案内する）。worker が `pending` と答え続ける間は 1 時間まで待つ（worker 側は既定 15 分で失敗にする）。導入を始めるときに stderr へ 1 行出す。導入の失敗はその版を blocked にして残りを続け、導入後の測り直しで見えない版は `envsync::verify` が blocked にする。resident なし・旧 peer・応答なしの打ち切りは、その場で sync を終える（それまでに導入したものはメッセージに含める）。worker から来た文字列は制御文字を除いて出す。
- **表示の食い違いの修正**: 成功時の 1 行は `render_sync` が作り、`in_sync` が true のときだけ「is in sync」と言う。false のときは「is not in sync with the parent」で始め、`--project` を付けたときだけ「プロジェクトの要求は満たしている」と続ける（以前は `in_sync: false` で「meets the project's requirements」とだけ言い、`--project` なしでも project に触れていた）。

A44 の sync 側のテスト（ネットワークに出ない）:
- `tests/it/bootstrap.rs`: 本物の resident と local worker、偽の nodenv manager で、要求 → 導入 → 測り直し → in sync の往復、2 回目は何も実行しないこと、導入済みは `unchanged`、resident が無いときは導入も起動もしないこと、manager の失敗が blocked になること、同時の 2 回の sync で manager が 1 回しか走らないこと。
- `tests/it/worker_agent.rs`: feature を合意していない接続の拒否、上限と重複、registry に無いものを導入しないこと、1 つの warm-up が 1 つの consumer であること、決着後の同じ答え、時間切れ、tick による解放、停止中と満杯。
- `src/coordinator/env_sync.rs`: 旧 peer に要求を 1 回も送らず `workers upgrade` を案内すること、応答なし・拒否・不足した応答の区別。`src/service/workers/warmup.rs`: 応答の解釈（純粋関数 `look`）。`src/toolchain/registry/tests.rs`: warm-up の要求と attempt の要求が同じ install plan になること。

残り（S21 は完了していない）:
1. （7 本目で解消）`doctor` への組み込み。下の「S21 `doctor` への接続」。
2. ssh 越しの remote worker に対する `workers sync` の実機での受入（S26）。往復の試験は local worker の link で行った。remote の link を作る `config/runtime.rs` が feature の合意を渡すこと（合意した peer には送り、していない peer には送らない）は `tests/it/runtime.rs` で確かめたが、ssh の bridge を通した導入の往復は試験していない。
3. `--project` は workflow の `setup-*` 入力を読まない（版指定ファイルのみ）。（2026-11-23 追記: 読むようになった。済み。）
4. `diff` の worker 省略時の並列化、`installed_inventory` の所要時間の計測（1 本目から継続）。
5. （4 本目で解消）spec 11 章・18 章・22.2 に `env_sync` の意味、feature `env_sync_v1`、N/N+1 の規則を書いた。
6. sync の途中で Ctrl-C したとき、worker 側の導入は最長 `warmup_idle_ms` 続いてから止まる（即時の取り消し要求は無い）。

### S21 `workers sync` のレビュー修正（2026-11-17、4 本目: PR #59 の指摘）

- **H1（S20 からの欠陥）**: 失敗した導入・途中で止められた導入が receipt の無い private root を残し、同じ版がその後ずっと失敗していた。原因は 2 か所で、`poll_tool` が導入者を起動する前に `detect` の `installation_incomplete_or_unreceipted` を `Failed` にしていたことと、`install_admitted` が中身のある root を `installation_partial_requires_cleanup` で断っていたこと。直し方: `poll_tool` は「root はあるが receipt が無い」場合に限って導入者を起動する（信用できない receipt・tree の drift・安全でないディレクトリは従来どおり `Failed`）。`install_admitted` は `install.lock` を排他で取り（他の導入者が持っていれば何も消さずに待つ）、receipt があればそれを返し、無ければ root の中身を消してから入れ直す。消す前に root の場所を確かめる（管理ディレクトリの直下、名前が plan identity の 64 桁 16 進、所有者のみの権限、symlink を経由しない）。`install.lock` は残し、symlink は辿らずに外し、ファイルは mode を変えずに外し（外への hard link の先を書き換えない）、receipt のある root（検証できない receipt を含む）は消さない。仕事の中の導入も warm-up も同じ `poll_tool` → helper → `install_admitted` を通るので、両方が直る。修正前に、失敗させた版をもう一度導入するテストが builder と resident 経由の両方で失敗することを確かめた（`installation_incomplete_or_unreceipted`）。
  - 変わった既存の振る舞い: 導入が失敗した版を次の consumer が要求すると、導入をもう一度試す（以前は 1 回目の失敗を返し続けた）。失敗を見た consumer 自身には同じ失敗を返し続ける。`installation_busy` と `installation_partial_requires_cleanup` は返さなくなった。
- **M1**: 一度 `pending` を見た後は、resident に繋がらない（接続拒否・socket なし）、resident の「停止中」の答え、`NOT_FOUND`（resident が worker をまだ掴んでいない）を答えなしとして、2 分の上限の中で聞き直す。`pending` を見た後に止める・打ち切るときは、導入が worker で続いている可能性と `workers diff` を添える。resident は起動のたびに token を作り直すので、答えを読めなかったら client を読み直してから聞き直す（これが無いと、再起動後の resident に古い token で聞き続けて打ち切りになる。再起動をまたぐ試験で見つけた）。
- **M2**: 停止中の worker は新しい warm-up を `CONFLICT` で断り、親はその場で止めて worker の文言を出す（`OVERLOADED` は「混んでいる」だけになった）。error code は増やしていない（古い親が読めない値を返さないため）。resident の再起動の案内は、resident がこの操作に一度も答えなかったときだけ出す。
- **M3**: 親は worker の答え（`pending` と `OVERLOADED`）で「最後に答えがあった時刻」を更新する。worker の確認は `sync_id` ごとに 1 本の別スレッドで行い、warm-up の lock の外で動く。確認を始めた要求は最長 1.5 秒待って `pending` を返し、確認中に来た同じ `sync_id` の要求は分かっている結果をその場で返す（時間切れになった要求がスレッドを積み上げない）。遅い確認の結果は残り、次の要求に答える。確認中の warm-up は idle でも手放さない（手放すと、確認が consumer を作り直し、誰も解放しなくなる）。
- **L1**: worker が `unchanged` と答えた版は `installed` に入れない。測り直しと `verify` は、`unchanged` の版にも行う。親が `pending` を見た後の `unchanged`（worker の再起動で warm-up を忘れた場合）は、導入したものとして数える。
- **L2**: `toolchain_poll` は warm-up の lock の外（上の別スレッド）で呼ぶ。
- **L3**: resident の client を作れないとき、「resident が動いていない」と言うのは、token か socket が無い・接続拒否のときだけ。それ以外はそのままの理由を出す。
- **L4（入れない）**: warm-up は `max_concurrent_prepares` に数えない。この上限は workspace の準備（`host.prepare` のスレッド）の数を縛るもので、仕事の中の toolchain の導入もこの枠を持たない。warm-up が枠を持つと、最長 15 分の導入の間、workspace の準備が待たされる。導入の同時数は、版ごとの singleflight、task-traffic-controller の gate、warm-up の上限 16 で縛っている。
- **テスト（M4）**:
  - 導入中の worker の再起動: `tests/it/toolchain_preparation.rs`（同じ state を開き直し、古い導入者が片付くまで次を始めず、その後に導入が成功する）。
  - resident の再起動をまたぐ sync: `tests/it/bootstrap.rs`（local worker は resident の中にあるので、worker の再起動も兼ねる）。
  - 中断のあとの 2 回目の sync（worker が忘れる前）: `tests/it/bootstrap.rs`（manager は 1 回だけ）。忘れた後（60 秒）は、`warmup_idle_ms` を resident の設定から変えられないため resident 経由では試験していない。同じ経路を 2 段で確かめた: agent が idle で参照を手放し、同じ `sync_id` の聞き直しが新しい warm-up になること（`tests/it/worker_agent.rs`、link の切断が `warmup_idle_ms` を超える場合も同じ）、参照が無くなって止められた導入の後に同じ版が導入できること（`tests/it/toolchain_preparation.rs`）。
  - gate: `tests/it/bootstrap.rs`（worker の予算を別の entry で埋めると、導入が worker の ledger に並び、manager が走らない。空けると導入される）。
  - 遅い確認: `tests/it/worker_agent.rs`（確認が戻らなくても要求は `pending` で返り、聞き直しは確認を増やさず、結果は残る）。

残り（S21 は完了していない）: 4 本目の 1〜4 と 6。加えて、`warmup_idle_ms` を超えた後の 2 回目の sync を resident 経由の 1 本の試験で確かめること（設定の口が要る）。owner の read が無く空でないディレクトリを残した root は、rename で空になるので次の導入は進むが、その trash は消せずに残る（手で消す）。root の中を path で名指しして書く生き残りの子孫は、新しい root に書き込める（trash への rename では防げない）。

### S21 `workers sync` のレビュー修正（2026-11-17、5 本目: 削除の経路と失敗の記憶）

PR #59 の H1 修正（4 本目）へのレビューを受けて、toolchain の root を消す経路を固めた。

- **M1（失敗した版のやり直し）**: 導入者が自分で失敗した版を key ごとに 10 分（`SetupConfig::tool_failure_hold_ms`）覚え、新しい consumer には導入をやり直さずに同じ失敗と残り秒数を返す。覚えるのは helper が失敗の exit で終わった場合、成功の後の `detect` が通らなかった場合、制限時間切れだけで、cancel・消失・signal による終了は覚えない（`policy::install_itself_failed`）。HostTools の digest が変わったとき、warm-up の最初の look（`Host::toolchain_retry`）、worker の再起動で解除する。判定は純粋関数 `policy::failure_held`。
- **L1（fd を基準にした削除）**: `remove_at` は `openat(O_DIRECTORY|O_NOFOLLOW)`・`fstat`・`fchmod`・`unlinkat` だけでたどる。path の chmod と `read_dir` は無い。`st_dev` が root と違うディレクトリは `EXDEV` で止まる。`managed_root` は管理ディレクトリと root を開いたまま返し、clear はその descriptor を使う。読めない（owner の read が無い）ディレクトリは、空なら外し、空でなければ止まる（path で chmod し直さない）。
- **L2（trash）**: clear は `install.lock` 以外を `.trash-<root名>-<ULID>` へ rename してから消す。rename の失敗は導入の失敗で、何も消さない。rename の後に trash を消し切れなくても導入は続け（別の device に当たった場合だけ失敗）、trash は残す。残った trash は同じ root の次の clear（lock の下）と、起動時の `adapters::sweep_trash`（その root の lock が待たずに取れた場合だけ）で消す。root そのものは rename しない（`install.lock` の inode が排他そのものなので）。
- **L3（期待する親）**: `install_admitted` と `managed_root` は管理ディレクトリを引数で受け、`root.parent() == expected` を、何かを作る前に確かめる。
- **L4・L5（緩めた試験）**: helper を殺した後の試験を `calls()==2` と「2 本目が Pending」に戻し、失敗時は理由を出す。「すぐ Failed」の原因は helper の 3 秒固定の承認待ちだった（混んだ全体実行で `installer_not_admitted` が 1 件余分に出て、導入が manager を動かす前に「a setup step exited with 1」で終わった）。待ちを 60 秒にし、承認の記録が別の process を子と名指ししているときだけ即座に諦める（`preparation::admission_step`）。restart の試験は、unsettled が空になった直後に古い子が死んでいることを確かめる。
- **dry_run**: `env_sync` の dry_run は receipt の無い root を `would_install` と答える（本番の sync と同じ）。
- spec §22.2 の削除の条件を、上に合わせて直した。

残り: 上の 4 本目の残りと同じ。

### S21 `workers sync` のレビュー修正（2026-11-17、6 本目: 覚える失敗を狭める）

5 本目の M1 は覚える範囲が広すぎた（helper の exit 1 のすべて、phase を問わない時間切れ、helper の成功の後の I/O エラー）。次に狭めた。

- helper は、導入そのものの失敗のときだけ専用の exit code 3（`preparation::INSTALL_FAILED_EXIT`）で終わる: manager のコマンドの失敗（`installation_command_failed`）、probe の不一致（`installed_version_mismatch`）、別の device（`installation_cleanup_other_device`。これまでは `installation_partial_cleanup` と同じ文字列だった）。分類は純粋関数 `preparation::install_exit`。それ以外は今までどおり exit 1。
- `policy::install_itself_failed` が覚えるのは、その exit code と、phase が Running の時間切れだけ（`BuildEnd::TimedOut` が phase を持つ）。承認待ちの時間切れ、exit 1、helper の成功の後の `private_tool_root`・`detect` の失敗は覚えない。
- spec §22.2 の覚える条件を、上に合わせて直した。

残り: 上の 4 本目の残りと同じ。probe の出力が読めない（`probe_*`）・出力が長すぎる（`installer_output_limit`）は exit 1 のままで覚えない。Running の時間切れには `install.lock` を待つうちに切れた場合も入る。

### S21 `doctor` への接続（2026-11-17、7 本目。main の取り込み後）

main（S23 の doctor・upgrade、scope の統一、schema v11 など）を merge commit で取り込み、`doctor::command` が `doctor::run` の `diff` に渡す値を `None` から `workers diff` の比較に置き換えた（spec §22.2「`doctor` はこの結果を含む」）。

- 答えた worker ごとに、`workers diff` と同じ経路（`workers::fetch_env` が登録済みの binary の `worker-service env` を ssh で尋ねる）で環境 report を取り、`envdiff::diff` の結果を check にする（`doctor::worker_diff`）。親の測定は 1 回だけで、worker が 1 台も答えなければ測らない。親を測る環境は `workers diff` と同じにした（常に保持した service の env。呼び出した shell の env では測らない。`doctor::measure_parent`）。
- 項目から check への変換は純粋関数 `doctor::diff_checks`。id はどれも `environment_diff` で、項目ごとに 1 check（detail は `<項目>: <分類> parent=… worker=… -> <手順>`）。`same` は ok、`differs`・`missing`・`manual_required` は fail（利用者が手を動かさないと直らない差）、`auto_installable` は notice（exec の warm-up と `workers sync` が入れる。detail で `belt workers sync <worker>` を案内）、`not_measured` も notice（「まだ測っていない」。`not_checked` にしない）。report を取れなかった worker（古い binary、壊れた応答、拒否）と親を測れなかった場合は、理由を添えた `not_checked` 1 つで、空の結果にも ok にもしない。worker から来た文字列は制御文字を除く。
- ssh で答えなかった worker には環境を尋ね直さない（`reachable` が既に fail を出している。2 回目の待ちを作らない）。`environment_diff` は `not_checked` として列挙する。
- 試験: `src/service/doctor/tests.rs` の `environment_diff`（全分類、制御文字、取れなかった場合、偽の ssh 越しの差と一致、親を測れない場合、答えない worker）と、実 binary・偽 ssh の `tests/it/doctor.rs`（worker の platform が違う report を返すと `environment_diff` が fail になり、policy digest は notice で `not_checked` は 0）。差の無い親・worker の doctor が exit 0 になること（`a_registered_worker_with_nothing_to_fix_keeps_the_exit_at_zero`）と、親を保持した env で測ること（`the_parent_is_measured_in_the_retained_environment_as_workers_diff_does`）も固定した。

S21 で終わったもの: `workers diff`（1 本目）、`workers sync` の計画・blocked・導入と warm-up（2〜6 本目）、`doctor` への接続（この節）。

S21 の残り（必須。S21 は完了していない）:
1. policy digest（task-traffic-controller の実効 policy）を `envreport::collect` が測らない（常に `None`）。diff は `not_measured`、doctor は notice（exit に影響しない）になる。測るようにするのは残り。（2026-11-23 追記: `envreport::collect` が policy digest を測るようになった。この項は済み。）doctor 自身の `traffic_policy` は host ごとの 2 つの scope を比べるだけで、親と worker の間は比べていない。
2. （解消）doctor の区分は決定済み: fail は `differs`・`missing`・`manual_required` と disk 不足、notice は `auto_installable` と `not_measured`、`not_checked` は到達できなかった worker と親を測れなかった場合。
3. doctor（`worker-service doctor`）は journal に記録した service の env で事実を集め、diff（`worker-service env`）は worker.json の env で測る。読む場所が 2 つあり、doctor 側は `XDG_CONFIG_HOME`・`XDG_STATE_HOME` を除く。2 つが同じ測定になることは確かめていない。
4. （解消）親は doctor も `workers diff` も保持した service の env で測る。呼び出した shell の env には落とさない保持された env が無い（PATH が無い）と `workers diff` は測定を拒否し、doctor も同じ理由で「親の環境を測れない」の `not_checked` にする（`tests/it/doctor.rs` の `without_a_retained_service_environment_...`）。
5. doctor は worker ごとに ssh を 3 回（doctor・hello・env）直列で行う。`diff` の worker 省略時の並列化と合わせて未着手。（2026-11-23 追記: worker の検査は並行になった。1 台の中でも、doctor の答えのあとの bridge `hello` と環境の比較は同時に尋ねる。済み。）
6. 3 本目の節の残り 2〜4 と 6、4〜6 本目の節の残り（上の各節）。`--project` は workflow の `setup-*` 入力を読まない。`installed_inventory` の所要時間は未計測。（2026-11-23 追記: `setup-*` 入力は読むようになった。済み。`installed_inventory` の所要時間は未計測のまま。）
7. 実機での受入（A44 の native 検証、S26）。

### S9a/S10 追補: worker と task-traffic-controller CLI の scope 統一（2026-11-17）

worker は自分専用の scope（`<state>/traffic`）を使っていて、同じ host の CLI（`$XDG_STATE_HOME/task-traffic-controller`、無ければ `~/.local/state/task-traffic-controller`）と別の ledger・budget になっていた（仕様 §14 の「同じ ledger・lock・lease」「nested にも同じ scope」と §18 の「既存の scope を検出して同じ budget を使う」に反する）。CPU を二重に使い、step の中の `task-traffic-controller exec` は自分の run を見つけられず別 job として gate されていた。

- scope の規則は `src/config/traffic.rs` の 1 か所に置いた（0.8.2 の時点では library に公開関数が無く、ここに書き写した。0.10.1 で `Paths::from_env` に置き換えた。下の「依存更新」）。`XDG_STATE_HOME`（空は未設定扱い）、無ければ `HOME/.local/state`。config も CLI と同じ `XDG_CONFIG_HOME` / `HOME/.config` の `task-traffic-controller/config.toml` にした（policy を worker だけ別にしない）。CLI より厳しい点が 2 つある。相対パスは拒否する（CLI は自分の cwd で解決するので、worker と step で一致させられない）。`HOME` も無ければ拒否する（CLI は `/` に落ちる）。
- 適用は `ResolvedConfig::worker_runtime` の 1 か所で、worker 前景 role と resident の local worker の両方が通る。agent・forecast・supervisor・setup builder は同じ値を受け取る。`AgentConfig::new` / `SetupConfig::new` の既定（`<state>/traffic`）は変えていない（直接組み立てる試験が利用者の scope を触らないため）。
- step の環境: CLI に state directory を直接渡す変数は無く、`HOME` と `XDG_STATE_HOME` だけで決まる。`XDG_STATE_HOME` を profile の保護対象にした。worker が持っていれば明示 profile の allowlist に無くても step へ渡し、step・profile の定数からは設定も変更もできない（worker が持たなければ step も持てない。worker と同じ値を書くことだけは通る）。worker と違う値を設定する workflow は配置時に拒否される。持ち方は下の「レビュー指摘の修正」で変えた（toolchain の環境とは別に持つ）。
- 移行: 起動時に旧 `<state>/traffic` を見る。無ければそのまま。ledger が読めない、または reaper が保持する entry（lease 保持中、または process が残りうる managed command）があるあいだは serve しない。旧 directory は消さず動かさない（旧 scope で開始した attempt の保存済み spec がその path を持つ）。履歴は取り込まず cold start とする（§18 が許容）。待ち方と、未開始の attempt の扱いは下の「レビュー指摘の修正」。
- 試験は一時 directory だけを `HOME` / `XDG_STATE_HOME` にする。規則と保留の判定は `src/config/traffic.rs` の単体試験、profile の保護は `src/toolchain/profile.rs`、worker_runtime の値と旧 scope の拒否は `tests/it/runtime.rs`。nested は `tests/it/supervisor.rs` で、worker_runtime が決めた scope と step 環境で supervisor に test binary 自身を step として実行させ（`nested_cli_probe`）、その中で CLI の規則（試験側に独立に書いたもの）と CLI と同じ `exec::nesting` を使い、同じ ledger に worker の entry が見えて、その entry の下で動く（inherit）と判定されることを確かめる。旧 scope では見つからないこと（top）も対照として確かめる。
- 実際の CLI を step から呼ぶ試験は `a_real_nested_cli_runs_under_its_step_and_shares_the_ledger`（下の「レビュー指摘の修正」の 6・8）。

依存更新（task-traffic-controller 0.8.2 から 0.10.1、2026-11-17）で直したこと:
- 0.10.1 の library は `Paths::from_env(home, xdg_state_home, xdg_config_home)` を公開した（CLI が呼ぶのと同じ関数）。`src/config/traffic.rs` の `resolve` はこれを呼ぶだけにして、規則を二重に持たない。belt 側の制約（相対パスと `HOME` 無しの拒否）だけを呼ぶ前の検査として残した。
- 0.9 以降の CLI は nested exec の間 ledger を v3 形（`entries_v3` のみ）で書く。0.10.1 の `Ledger::load` はこれを読めるので、supervisor の `run` と forecast の inspect が `Traffic("ledger: ...")` で失敗する問題は解消した。試験: v3 形の ledger を scope に置いて run が成功し v3 形のまま残ること（`tests/it/supervisor.rs`）、forecast が読んで何も変えないこと（`tests/it/forecast.rs`）。導入済みの CLI を step から実際に呼ぶ試験は、下の「レビュー指摘の修正」の 8 で CI に入れた。
- service 環境の allowlist（worker・worker-service・init）に `XDG_STATE_HOME` を足した。install と init の時点で利用者の shell の値を journal の env に取り込み、常駐 worker が同じ scope を使う。launchd の plist は `PATH`・`HOME` などの固定の鍵しか受けないので、`XDG_CONFIG_HOME` と同じく plist には渡さない（worker は journal の env から読む）。install と init は、worker が起動時に拒む env（相対パスの XDG と `HOME`）をその場で拒む。
- step への環境: `XDG_CONFIG_HOME` も `XDG_STATE_HOME` と同じく profile の保護対象にした（worker の値を明示 profile の allowlist に無くても渡し、step・profile からは変更できない）。toolchain adapter の `activation_env` は `XDG_*` をすべて無視する。

レビュー指摘の修正（#56・#58、2026-11-17）:
1. 受理済みで未開始の attempt（high）。保存済みの `supervise.json` は受理・準備の時点の scope（`traffic_state_dir`・`traffic_config`・node の env）を持つ。**書き換える方を選んだ**。この 3 つは worker が spec を書くときに自分で決める値で、payload にも plan digest にも execution binding にも入っていないので、書き換えても受理した契約は変わらない。agent は Ready の attempt を起動する直前（`rewrite_start`、lease の seed と残り deadline を書くのと同じ場所）に `SuperviseSpec::rescope` で今の scope に合わせる。install 後に `XDG_STATE_HOME` が変わって scope が A から B に移った場合も同じ経路で B になる。試験: `tests/it/worker_agent.rs` の `an_attempt_stored_for_another_scope_starts_in_the_scope_of_the_worker`（起動直前に保存済み spec を旧 scope の形に戻し、command が worker の scope の ledger で admit され、環境が worker の値になることを確かめる）。
   - 書き換えないもの: command を開始した可能性のある attempt（`Recovery::Supervise`）。その supervisor は command を二度と起動せず、admit された ledger から残った process を探して止めるだけなので、保存された scope のまま回復させる（書き換えると探せなくなる）。setup builder の `spec.json` は書いた直後に同じ owner が起動し、再起動をまたいで起動し直すことは無い（前の owner の builder は `settle_old` が止めて片付ける）ので、書き換えの対象は無い。新しく書く spec は agent・builder とも今の scope になる。
   - 旧 scope のまま生きている supervisor（agent が落ちても動き続け、残りの node を旧 scope で起動しうる）があるあいだは serve しない（下の 3）。旧 `<state>/traffic` の ledger だけでなく、保存済み spec の `traffic_state_dir` が今の scope と違い、supervisor lock が取れない attempt・builder を数える。
2. env が最初の install で固定される（medium）。service file を登録したあとの `install` は journal の env を使い回すので、「install し直せば直る」は誤りだった。直し方: `worker-service stop` のあと同じ binary で `install` し直すと、未終了の attempt が無い場合に限り、journal と `worker.json` の `XDG_STATE_HOME` / `XDG_CONFIG_HOME` だけを呼び出し側の値（未設定を含む）に置き換える。`PATH` などほかの値と service file は変えない。動作中の worker、未終了の attempt がある worker は元の値のまま。`worker-service status` は journal の env から解決した scope を `traffic_scope`（`state_dir`・`config_file`）で返す（呼び出し側の shell の値ではない）。
3. 拒否で worker が終了し launchd が再起動を繰り返す（medium）。終了しない。worker 役と resident は `worker_runtime` の前で待ち（`src/service/traffic_hold.rs`）、1 秒から倍にして 30 秒までの間隔で見直し、解けたら起動を続ける。記録は状態が変わったときだけ出す。SIGTERM / SIGINT では待ちをやめて終了する。待つあいだ `<state>/traffic-hold.json` を毎回書き直し、`worker-service status` が `traffic_hold`（`reason`: `waiting_for_previous_scope`）で返す。90 秒更新されていない記録と、service が load されていないときの記録は、いなくなった worker のものとして返さない。ledger が読めない場合は待っても直らないので別の `reason`（`previous_scope_unreadable`）と別の文面にし、手順（worker が起動した command が残っていないことを確かめ、`<state>/traffic` を別名へ動かす）を示す。動かせば次の見直しで起動する。`worker_runtime` 自身の検査は、待たない呼び出し側のための最後の確認として残した。
   - 待っているあいだは agent が動かないので lease を更新する相手がいない。旧 scope で動いている attempt は、自分で終わるか、lease が切れた時点で supervisor に止められる。supervisor が生きているかぎり、待ちが lease より長く続くことは無い。supervisor が死んで command だけが旧 ledger に残っている場合は、その process が終わるまで待つ（回復する agent が動いていないため。以前の拒否でも同じだった）。
4. scope の変数と toolchain の環境（medium）。`XDG_STATE_HOME`・`XDG_CONFIG_HOME`・`TASK_TRAFFIC_CONTROLLER_NESTED` は profile の digest と fingerprint、execution binding の activation から常に外す。scope の場所を伝える値で toolchain の環境ではなく、入れると scope を動かしただけで受理済みの binding と計測履歴が無効になるため。profile は toolchain の環境（`environment()`）と scope の変数（`scope_environment()`）を別に持ち、command の環境は `command_environment` が両方から作る（binding があれば activation の上に worker の今の scope を載せる）。worker 自身が使う補助（tool の probe・git・workspace の道具）には、step と同じく両方を渡す（`host_environment()`。`XDG_CONFIG_HOME` を読む tool が probe と step で違う答えを返さないため）。setup の key を作る `SetupConfig.env` には入れず、`scope_env` として setup step へ載せる（tool の installer は plan が決めた専用の `XDG_*` を持つので載せない）。belt は未公開で配備済みの worker がいないので、`XDG_CONFIG_HOME` を持っていた worker の fingerprint が 1 回変わることは受け入れた。試験: profile の digest・fingerprint・環境が scope の変数の有無で変わらないこと（`src/toolchain/profile.rs`）、受理時の activation と、worker が scope の変数を持つようになったあとに作り直した activation が等しいこと（launch と回復が行う比較。`src/toolchain/adapters/tests.rs` の `an_accepted_binding_stays_valid_when_the_worker_gains_scope_variables`）。
5. #56 と #58 は一緒に main へ入れる（#56 だけでは CLI 0.9 以降の v3 ledger を 0.8 の library が読めない）。
6. nested exec（low）。仕様 §14（763 行）は「同じ scope の lease を検証して継承し、二重課金を避ける」で、inherit である（773・775 行は scope と policy の一致を述べるだけで、どちらとも決めていない）。CLI は 0.9 から既定が admit（step の entry の子として budget に並ぶ）なので、worker は全 step に `TASK_TRAFFIC_CONTROLLER_NESTED=inherit` を渡し、profile・step からは変えられないようにした。nested の command は step の entry の下ですぐ動き、自分の entry を持たない。試験: probe が inherit と判定すること、worker の変数を外すと child（admit）になること（対照）、実 CLI で nested 実行中の ledger に step の entry しか無いこと。
7. `src/forecast/plan.rs` の `overrunning()` は `parent` を持つ entry を数えない（親の中で動く。利用者の CLI が admit で作る子 entry は履歴を持たないことが多く、数えると overrun の理由が誤って出る）。
8. 実 CLI の試験（low）。`#[ignore]` をやめ、`BELT_REQUIRE_FAST_WORKTREE` と同じ規約にした（CLI が PATH に無ければ理由を出して skip、`BELT_REQUIRE_TTC_CLI=1` なら失敗）。設定は CLI が読む `HOME/.config/task-traffic-controller/config.toml` に書き（budget 2、CPU・memory の guard 無し。admit の回だけ budget 3: admit では nested の command が step とは別に budget を 1 つ使うので、2 のままだと並行する別の attempt が admit されない。inherit が避けているのはこの二重計上である）、同じ file を worker の `traffic_config` に渡す。worker が step に渡す環境（inherit）と、`--nested admit`（v3 形の ledger を worker の library が読み、別の attempt を並行して supervise できること）の 2 通りを回す。別の attempt は期限付きで待ち、nested の command は試験が失敗しても必ず解放する（解放前に `run` を呼んで止まる順序を無くした）。CI は test job（ubuntu・macos）に CLI 0.10.1 を入れ、`BELT_REQUIRE_TTC_CLI=1` を設定する。coverage job と mutation job には入れない: coverage の gate は `scripts/pure-modules.txt` の module の行だけを見ていて、この試験が通る supervisor の経路は対象外であり、mutation は `--lib` と `config::` の統合試験だけを選ぶ。どちらでも CLI が無ければ skip され、gate の値は変わらない。
9. `traffic::resolve` の検査は `worker-service install` と `init` だけに掛ける。stop・drain・resume・status・start は、呼び出し側の shell の `XDG_STATE_HOME` が相対でも動く。
10. `Cargo.toml` は `~0.10.1`、README は「0.10.x」。ledger を同じ user の CLI と共有するので、ledger の形式が変わりうる次の minor へ library だけ、または CLI だけが進まないようにする。
11. 既知の制約: worker は利用者の CLI の設定 file を読む。0.10 の library の検査が拒む key や値があると、直すまで worker が起動する command はすべて失敗する（README に記載）。

残り:
1. `worker-service status` の `traffic_scope` と、実行した shell が解決する scope・policy digest を比べて警告する処理は doctor（S23）の残件。status は値を返すだけで比べない。
2. resident（`init` で入れた親側）: 待ち（上の 3）は入っているが、`traffic_hold` を返す status が無く、`init` のやり直しで journal の scope を更新する経路も無い。`workers` の親側の表示も `traffic_scope` / `traffic_hold` をまだ出さない。
3. 待ちの SIGTERM / SIGINT の経路は試験していない（試験 process へ signal を送らないため）。
4. 実機での upgrade（旧 scope で attempt が動いている worker を入れ替える）は S26 の受入で確かめる。

### S23 workers upgrade と rollback（2026-11-17）

`workers upgrade <worker>` を CLI へ接続した（`Envelope::not_implemented` に落ちなくなった）。`doctor` は次の変更で接続する。

- worker host の helper に hidden の `worker-service upgrade`・`rollback`・`commit` を足した（`src/service/worker_service/upgrade.rs`）。`upgrade` と `rollback` は「目標の版へ切り替える」同じ再開可能な手順で、順序は drain（upgrade が drain したことを journal に記録）→ 配置（版別 directory、既存 file は上書きしない）→ 互換検査（新しい版が worker.json・identity binding・worker DB を読めるか。DB は互換表つきの read-only open で、その場で migrate しない）→ live attempt が 0 になるのを待って旧 service を bootout → worker DB を checkpoint して `bin/<切替元>/worker.db.saved` へ退避 → journal へ切替を記録 → 旧 plist を receipt で確かめて削除 → worker.json → `bin/current` を rename で付け替え → 新 plist を journal つきで公開 → 起動、である。途中のどの step で落ちても、`upgrade` の再実行で完了し、`rollback` で元に戻る（全 step への失敗注入で確認）。版ごとの worker.json は `bin/<digest>/worker.json` に bytes のまま残し、戻すときはその bytes を使う。
- **切替の記録は commit まで残す。** journal の `switch`（切替元の版、まだ消していない旧 plist の receipt、DB を退避したか）は、新しい版を起動しても消さない。親が health を確かめた後に送る `commit` だけが、記録と退避した DB を消し、upgrade が行った drain を resume する。commit の前なら、親が落ちた後の再実行でも前の版へ rollback できる。rollback は、前の版を起動する前に退避した DB を戻す（新しい版が migrate していても前の版が開ける）。戻し終えた rollback は記録を自分で消す。commit の後は rollback できない（helper が拒否する）。静止時（commit 後）の journal と status は S22 の版がそのまま読める形のままである。
- **drain の持ち主を host に残す。** `upgrade` は、worker が drain 中でなければ journal に `drained_by_upgrade` を書いてから drain marker を書く。すでに drain 中ならそれは利用者のもので、記録しない。`commit` は記録があるときだけ resume する。利用者が `workers drain` / `workers resume` を打てば、その drain は利用者のものになる（記録を消す）。切替が commit 待ちの間の `workers resume` は拒否する（退避した DB へ rollback し得る間に仕事を受けると、その記録を失うため）。
- **host の排他。** `upgrade`・`rollback`・`commit`・`drain`・`resume` は、journal を読む前から答えるまで `worker-service.lock` の排他 flock を持つ。取れなければ何も変えずに「切替中」と失敗する。`status` は共有 lock を試し、取れなければ `switch_running` を付けて答える。
- 親側（`src/service/workers/upgrade.rs`）は、resident の確認 → staging（digest 照合。壊れた転送は公開されない）→ status → helper の `upgrade`（drain を含む）→ health → `commit` → 登録の更新、の順に行う。失敗の報告は 3 つに分ける。切替が始まる前の失敗は rollback を呼ばず、前の版に `hello` を通してから「upgrade されていない、前の版が動いている」と報告する。切替が始まった後の失敗は `rollback` し、前の版にも health を通してから「rollback して前の版が動いている」と報告する（exit は CONFLICT）。rollback も失敗したとき・host を観測できないときは、最後に観測した状態（版・commit 待ちの切替・service が loaded か・agent が ready か・drain とその持ち主）をそのまま言い、「upgraded」「stopped」「serving again」のどれも言わない。health を通っていない版では drain を解かない。host が「切替中」と答えたときは、rollback も resume も送らずに失敗を報告する（待たない）。利用者が先に drain していた worker は upgrade 後も drain のままにし、そう報告する。
- **resident との整合。** `workers upgrade` は最初に、動いている resident へ build（起動時に読んだ実行ファイルの digest）と、`hello` で提供・要求する feature を尋ねる（resident の IPC に `Build` を追加）。resident が親の binary と違う build のとき、または答えられないとき（この問いを知らない古い resident を含む）は、何も送らずに拒否し、resident の再起動を案内する。resident が動いていなければ進む。health の `hello` は resident が要求する feature で行う。前の版は、upgrade の前に `hello` で合意していた feature だけを要求して検査する（前の版に尋ねられなかった再実行では何も要求しない）。互換の方針（N／N+1）は仕様 §18 に書いた。
- 親自身の更新との順序: この実装は「親の binary（この CLI 自身）を worker へ配る」ので、親を更新し、resident を新しい binary で起動し直してから `workers upgrade` を実行する。

受入項目との対応: A34 のうち upgrade・rollback・失敗からの再開・実行中 binary を上書きしないこと（`bin/<旧 digest>/` の bytes が全経路で変わらないこと）を、偽の Launcher・偽の ssh・一時 HOME で試験した。A36 のうち S23 の分は「静止時の worker-service journal と status JSON が S22 の版でも読める形のままであること」（旧 schema の厳格な読み取りで検証）と、既存の `workers` 系コマンドの統合試験が変わらず通ることである。

S23 の残り（必須。この変更で S23 は完了しない）:
1. `doctor` と A05（次の節）。
2. 本物の launchd・実 SSH・実機での upgrade / rollback（A34 の native 検証、S26）は未実施。
3. resident 自身の更新手順（init の「changed bytes は別の upgrade flow」）は未実装で、S23 の範囲外として残る。`workers upgrade` は build の違う resident が動いていると拒否して再起動を案内するだけで、resident を再起動しない。
4. rollback で戻した版の plist は、新しい版の helper が描いた内容である。plist の描き方が版の間で変わると、戻した先の古い helper は「installed plan differs」で start / stop を拒否する（現時点では描き方は変わっていない）。
5. live attempt があるあいだ upgrade は待ち（既定 600 秒）、時間切れなら何も切り替えずに失敗する。稼働中の attempt を旧版の supervisor のまま残して agent だけを切り替えることはしない。実 SQLite に live attempt を持つ worker での待ちと「切り替えていない」報告の end-to-end 試験は無い（文言の組み立てだけを単体で試験）。
6. rollback は commit の前に限る。試用中（起動から commit まで）の版が worker DB に書いた記録（ack・GC の進み）は、rollback で切替前へ巻き戻る。その間 worker は drain 中で新しい attempt は来ないので、これは許容する（仕様 §18 に明記）。退避は worker.db だけで、state directory のほかの file は戻さない。
7. host が「切替中」のとき、親は待たずに失敗を報告する。ssh が切れた後も helper が動き続けた場合は、終わってから `workers upgrade` を打ち直す。
8. lock を取らない旧版の helper（S22 の `start` / `stop`、この変更より前の `upgrade`）とは排他にならない。`install`・`start`・`stop` も lock を取らない。
9. レビュー指摘のうち、worker の task-traffic-controller の scope（M4）は別の変更で直す。実際の launchd 環境での検査（L3）は S26 の実機受入の残件とする。

### S23 doctor（2026-11-17）

`doctor` を CLI へ接続した（`src/service/doctor.rs`）。各 host は事実だけを集め、判定は親が 1 か所（`evaluate`・`shared_engines`・`service_checks`）で行う。worker host の事実は hidden の `worker-service doctor` が、journal に記録した service の環境（LaunchAgent に渡した env。`XDG_CONFIG_HOME`・`XDG_STATE_HOME` を除く）で集める。親自身の local worker は `coordinator.json` に保持した env で調べ、保持が無ければ呼び出した shell の env で調べたことを notice として出す。check の結果は `ok`・`notice`・`fail`・`not_checked` の 4 つで、見られなかったものを `ok` にしない。`healthy` は `fail` も `not_checked` も 1 つも無いときだけ true である。1 つでも `fail` があれば exit は BLOCKED（`blocked_reason = environment_mismatch`）、`fail` は無いが `not_checked` があれば exit 1（`COMMAND_FAILED`、`blocked_reason` なし）で、両者を exit code で区別できる（仕様に doctor の exit code の定めは無い）。`--json` はどの場合も `result` に全 check を載せる。

| check | 内容 | fail にする条件 |
|---|---|---|
| `traffic_runtime` | service の PATH が解決する `task-traffic-controller` に `status --json` を尋ね、`ledger_format`（managed entry を知っている binary だけが出す field）の有無を見る | field が無い（未修正の binary）、答えが読めない。command が無ければ ok |
| `traffic_state_path` | worker の scope（`<state>/traffic`）と、その環境で command が使う scope（XDG、無ければ HOME の下）を比べる | scope が違い、かつ command が導入済みか state を残している。どちらも無ければ notice |
| `traffic_policy` | 両 scope の実効 config の digest（task-traffic-controller 自身の `policy_digest` と同じ計算）を比べる | digest が違い command がある、config が読めない |
| `traffic_processes` | 実行中の task-traffic-controller process（旧い build が起動した job）の検査。この build では実装していない | fail にしない。command があるか state が残っていれば `not_checked`（ledger の entry 数を添える）。どちらも無ければ ok |
| `docker_resource_domain` | service の環境の docker client の接続先と engine ID | 接続先が unix socket / pipe 以外（別 host の engine）。engine が答えなければ notice、client が無ければ ok。engine は答えたが接続先が分からない（`DOCKER_HOST` が無く `docker context inspect` が 1 行を返さない）ときは `not_checked` |
| `docker_shared_engine` | 親と全 worker の engine ID を突き合わせる | 同じ engine を 2 host 以上が使う（両方に fail） |
| `service_profile` | service の環境で worker profile を load し、起動時と同じ probe を通す | load できない |
| `reachable`・`service`・`version`・`bridge`（worker のみ） | ssh で helper が答えるか、service が loaded / ready か、版が親と同じか、登録済み binary 経由の `hello` が同じ cluster と worker_id で完了するか（必須 feature の交渉を含む） | 到達不能・別 identity の答え・未 load・切替が未完または commit 待ち・hello 失敗。版の違いと drain 中は notice。host が切替中（`switch_running`）なら `service` は `not_checked` |
| `legacy_config`（project） | 実行した directory の project の `.local-ci.toml` に、値のある未対応 key（`link-node-modules` など）があれば notice | fail にしない。config を解決できなければ `not_checked` |
| `environment_diff`（worker ごと） | S21（`workers diff`）の結果。答えた worker ごとに `worker-service env` を尋ね、親との差を項目ごとに 1 check で載せる（上の「S21 `doctor` への接続」） | `differs`・`missing`・`manual_required` の項目。`auto_installable` と `not_measured` は notice（exit に影響しない）。比べられなかった worker は `not_checked` |

受入項目との対応: A05 のうち「未修正 binary を doctor が不適合とする」を、偽の `task-traffic-controller`（`ledger_format` を出さない `status --json`）で、library と実 binary の両方で試験した。A36 のうち S23 の分は、旧い形の `status --json` をそのまま読めること（読めたうえで不適合と判定する）である。

S23 の残り（必須。この変更でも S23 は完了しない）:
1. **A05 の「同じ budget・policy を共有」は、現状の既定では成り立っていない。** worker の supervisor は `<state>/traffic` を scope にしており（`AgentConfig::new`。設定項目は無い）、同じ user の `task-traffic-controller` command の scope（`~/.local/state/task-traffic-controller`）とは別の ledger になる。doctor はこれを、command が導入済みの host では `traffic_state_path` の fail として報告する（command を日常的に使う host では doctor が必ず fail する）。scope を揃える変更は S9a / S10 の範囲で、ここでは行っていない。A05 はそれが決まるまで未達である。
2. 「実行中 process」の検査（仕様の既存 state との互換の節）は未実装で、`traffic_processes` を `not_checked` として出す（process の走査はしていない）。command を導入した host では doctor は exit 0 にならない。（2026-11-23 追記: 実装した（PR #121）。単独 CLI の台帳にある、CLI が起動した job を `traffic_processes` の行に出す。belt が入れ子で登録したものは数えない。初期化済みで何も動いていない host は exit 0。済み。）
3. S21 の `workers diff` の結果は接続済み（上の「S21 `doctor` への接続」）。policy digest を親も worker も測らないが notice なので、差の無い親は worker を登録しても exit 0 になる（`fail` があれば BLOCKED、`not_checked` があれば exit 1）。
4. system-user mode（LaunchDaemon の `UserName`）の service user での profile 検査は未実装（S22 の残り）。Linux の linger の確認（仕様 §15）は S24 の親の側で入った（check `linger`）。
5. 本物の docker・実 SSH・実機の launchd 環境での doctor（A35 の「実際の service 環境で doctor が通る」）は未実施（S26）。偽の runner と偽の ssh は実挙動を確かめない。
6. 親の local worker の service 環境は、`coordinator.json` の env が空のとき呼び出し shell の env で代用する（notice を出す）。
7. レビュー指摘の残り: worker の task-traffic-controller の scope（M4。上の 1）は別の変更で worker 側を CLI と同じ規則に揃える。doctor の判定は変えていない。profile と command の検査は ssh の helper が journal の env で行い、launchd のドメインや Keychain の可否は再現しない（L3）。実際の launchd 環境での検査は S26 の実機受入の残件とする。
8. doctor は resident の build・feature と各 worker の照合をしていない（`workers upgrade` は行う）。bridge の `hello` は doctor を実行した binary の必須 feature で行う。
9. worker の traffic scope（`worker-service status` の `traffic_scope`）と実行 shell の scope の比較は doctor の残件（上の S9a/S10 追補の 1）。
10. 上の 1 と 7 の「worker の scope は別 ledger」は S9a/S10 追補で解消済み。merge で doctor の worker の scope を `traffic::resolve`（journal に記録した env）に合わせた。service の環境からは `XDG_CONFIG_HOME` に加えて `XDG_STATE_HOME` も外す（worker は journal から読む）。


### S16 の残件の分類（2026-11-17、main `22bbdcc` 起点）

S16 行の各項目を、main のコードとテストに照らして分けた。

| 項目 | 状態 | 根拠 |
|---|---|---|
| `ci plan`（FrozenPlan を 1 回だけ作る） | 実装もテストも済み | `service::planning::prepare` が 1 回の capture から `build_frozen_plan_from_source` を 1 回呼ぶ。実 CLI の `ci_plan_public_cli_frozen_worktree_commit_directory_and_private_failures` ほか 2 件 |
| 分割できない理由の表示（A28） | JSON は実装済みで、実 CLI のテストと text 表示が無かった。この変更で両方を足した | JSON は `PlanSummary` の `grouping`・`shard_groups[].not_split`・`depends_on`・`dependency_layer_width`。text は workflow・task・node の件数と digest だけだった。`ci::summary::render_text` と実 CLI の `ci_plan_shows_why_a_shared_workspace_task_is_not_split` を追加 |
| 複数 step・setup・matrix の forecast（A10 の S16 分） | 部品は実装もテストも済み、接続は未実装 | forecast の A10 は `forecast` の unit 4 件（`a10_*`）。`ci plan` は `plan_task_demand` の command step 数を出す。FrozenPlan の task を `WorkloadPlan` にして配置時の forecast へ渡す経路は無い（実行 driver と同時に要る） |
| matrix の global permit | 永続層は実装もテストも済み、利用側は未実装 | `matrix_group`・`matrix_permit` と `create_matrix_group`・`acquire_permit`・`release_permit`・attempt への束縛は storage の unit 2 件で検証済み。Coordinator は常に `permit_id: None` で submit し、group を作る呼び出しも無い |
| `ci`（実行）、Job DAG の配置、needs output | 未実装 | `CiSub` は `Plan` だけで、subcommand 無しの `ci` は `NOT_IMPLEMENTED`。worker と supervisor は argv の command だけを実行し、`src/ci/` の外で `execute_pinned_task`・`execute_task`・`execute_setup`・`TaskInputs`・`derive_job_dag`・`WireTaskResult`・`StepExecutor` を使う箇所は grep で 0 件 |
| dynamic matrix の展開 event | 受け皿だけ実装もテストも済み、展開そのものは未実装 | wire の `WorkloadPlan`・coverage と forecast は `expansion_seq` を運び、照合する（`forecast::plan` の unit）。展開を行って event に固定するコードは無く、scheduler が作る plan の `expansion_seq` は常に 0 |
| max-parallel・fail-fast・join・決定的な集約（A27） | 未実装 | group の進行を持つコードが無い。`fail-fast` は wire の skip 理由の文字列としてテストに出るだけで、`aggregate_matrix` の呼び出しは grep で 0 件 |
| host をまたぐ中間 artifact の receipt（A38） | 未実装 | `artifact_receipt` は attempt が終わった後の最終回収と `outputs` の書き出しだけが使う。実行中の upload を親が検証・保存して receipt を返し、別 host の download が待つ経路は無い |

未実装の分は、止まっている理由で 2 つに分かれる。

- local-ci の shared Driver 待ち: run の進行、動的 matrix の展開と event、permit の利用、max-parallel・fail-fast・join、cancel。上の 2026-11-15 の記録のとおり Driver が host に依存しない形で持つ。Driver は 2026-11-17 の時点で未公開で、crates.io の最新 0.17.1 にも `driver`・`job_evaluate` の module は無い。belt の依存は 0.13.0 のままである。公開済みの API だけで belt 側に進行を書くと、同じものを二重に持つことになる。
- Driver に依らず公開済みの API（`execute_pinned_task`・`TaskInputs`・`StepExecutor`・`plan_task_demand`）で作れるが、設計が未決定: (a) worker 側の CI task 実行。FrozenPlan・task id・`TaskInputs` を worker へどう渡すか（wire の新しい payload か別の形か）、step を task-traffic-controller に載せる `StepExecutor` を supervisor のどこに置くか。worker の admission と supervisor の spec に及ぶ。(b) FrozenPlan の task を `WorkloadPlan` にして配置時の forecast へ渡す接続（A10 の S16 分）。(c) 実行中の中間 artifact を親へ送り、receipt を別 host の download へ返す経路。

この変更では、どちらも実装していない。再開の順序の案は、(a)〜(c) の設計を決める、local-ci の Driver を現在の main へ載せ直して公開し belt の依存を別の変更で上げる、その上で (1) `ci` の実行 driver と worker 側の CI task 実行・needs output・A10 の接続・A38 のうち結果と output の分、(2) matrix の permit・max-parallel・fail-fast・join・集約と展開 event・A27、(3) 中間 artifact の receipt と A38 の artifact の分、である。(a) は Driver の公開を待たずに始められる。S16 の必須範囲は変えない。

### S16 (1) の前半: worker 側の CI task の実行（2026-11-17、main `cc4cc9c` 起点。同日、PR #64 のレビュー指摘を反映）

上の (a) の設計の決定（payload `CiTask`、feature `ci_task_v1`、supervisor が `execute_pinned_task` を呼ぶ）のうち、worker 側を実装した。親の側（`ci` の実行、DAG の進行、needs output の受け渡し、forecast への接続）と FrozenPlan の配送は次の変更で行う。

実装したもの:

- wire: `AttemptPayload.ci_task`（`CiTask { plan_digest, task_id, inputs }`）。無いときは書き出さないので、既存の payload の canonical JSON と hash は変わらない。交渉していない接続からの CI task は `PROTOCOL_INCOMPATIBLE` で断る。
- feature `ci_task_v1` は、この変更では `SUPPORTED_FEATURES` に入れない。FrozenPlan の配送と一緒に次の変更で提供する（plan を受け取れない worker が CI task を引き受けないようにするため）。worker 側の実装とテストは残し、テストは `worker::serve::serve_connection_offering` で feature を有効にした接続を組み立てて行う。
- worker の受理: CI task は `commands` を持たない。次のものは受理の時点で理由を付けて断る（`INVALID_INPUT`）。`in_place`・secret bundle・prepared setup（下の制約）。4 MiB を超える inputs。結果の artifact の名前 `belt-ci-task-result.json`（とその配下）を名前に持つ artifact・output の宣言。plan が届いていれば、`services:` を持つ task（下の残件）と、digest に合わない plan。
- agent（`src/worker/agent.rs` は振り分けの分岐 3 か所と、受理の `check_accepted`・GC の `gc_plans`・forecast の `answer` に state dir を渡す各 1 行。本体は `src/worker/ci_task.rs`）: 届いている FrozenPlan の bytes を digest で照合し、task がその plan に有ること・inputs が読めることを確かめてから、supervisor の spec に `ci` を入れる。`commands` が空でも `prepared_only`（command 無しの成功）にはしない。
- supervisor（`src/supervisor/ci_task.rs`、local-ci との接続は `src/ci/execute.rs`）: `execute_pinned_task` を別 thread で呼び、`StepExecutor` として受けた各 step を、exec の command と同じ `run_node`（task-traffic-controller の登録と admission、launch の claim、spool への mask 済みの出力、timeout、停止と停止確認）で 1 つずつ node として実行する。local-ci は step の process を起動も signal もしない（cancel handle には process group を渡さない）。service container の docker や git など、step 以外の補助 process は local-ci が自分で起動する。
- step の外の監視: core が step を求めていない間も、`poll_ms` ごとに取消要求・lease・attempt の deadline を見る（`Control::look_between_nodes`）。lease の更新は step の外でも反映するので、step の外の作業が lease より長くても、更新されている attempt は失効しない。止める理由が出たら、`ExecContext` に入れた `CancelHandle` で core を止める。
- attempt を終わらせるもの: cancel・lease 失効・attempt の deadline・資源待ちの timeout・spool の失敗・停止未確認。これらの後は `if: always()` の step も起動しない。step や job の `timeout-minutes`（と、上限としての親の `command_timeout_ms`。小さい方）が切れた step は、止めて停止を確認したうえで `StepEnd::Timeout` として core に返し、続けるかどうかは core が決める。
- 時間: `NodeEnd` が待ちの時間と実行の時間を分けて持つ。core へは `run_secs`（実行だけ）と `queue_wait_secs` を返す。
- 結果: task の結果（`WireTaskResult`。job ごとの結果と `outputs`）を、attempt の artifact `belt-ci-task-result.json` として既存の artifact と collection の経路で親へ返す。secret の値を含む結果は artifact として残さない。outcomes が下流の inputs の上限（`MAX_INPUT_BYTES`、4 MiB）を超える task は、実行した worker で失敗にする。step を 1 つも開始しなかった task は結果だけを残し、宣言済みの artifact と output は探さない（spec 13 章に例外として記載）。attempt の終了は、止める理由が有ればそれ、無ければ job の結果（failure か cancelled が 1 つでも有れば exit 1 の failed、skipped は失敗にしない）、task を実行できなかったときは exit 無しの failed と event `ci_task_failed`。
- plan の置き場 `<state_dir>/ci-plans/`: mode 0700（既に有るディレクトリも揃える）、256 件・合計 1 GiB まで。未完了の attempt が参照せず、置かれてから保持期間（`retention_ms`）を過ぎた plan は、attempt の GC（`Agent::gc`）と一緒に消す。
- 回復: lost した supervisor の step の FIFO は、`launches` の step の番号から引いて消す。step の command id は `step-N` の番号から引く（`recover::launched_index`）。
- forecast: 受理済みで未開始の CI task は、plan の `plan_task_demand` が挙げる command の数だけ `step-N` の future demand を出す（worker 側）。

テスト（network に出ない。実際の workflow ファイルと実 process）: `ci::execute` 4 件、`supervisor::ci_task` 9 件と `run_tests` 25 件（needs の連鎖、mask、失敗、digest 不一致、開始前の cancel、skipped、step の外で lease の 2 倍を超えてかかっても更新された lease が保たれること、待ちと実行の時間が分けて返ること、step の外での cancel・lease 失効・deadline、step の最中の cancel・lease 失効・deadline の後に `always()` が動かないこと、`timeout-minutes` の後に `if: failure()` と `if: always()` が動くこと、親の上限、`script_pipe` の step の error、secret を含む結果の拒否、step 無しの task の artifact、回復での FIFO と `step-N`）、`worker::ci_task` 14 件（services の拒否、予約名、inputs の上限、plan の上限・mode・GC、profile と binding の `base_env`）、forecast 2 件、wire 1 件、agent の統合 7 件（`Agent::gc` が plan を消すこと、未交渉の拒否、実際の agent と host から supervisor の binary まで通して needs の 2 task を成功させる 1 件、supervisor を kill した後の回復、services の拒否）。

この変更で分かった制約（local-ci 0.13.0 の公開 API による）:

- `PinnedExecution::verify_workspace` は、workspace が captured source と byte 単位で一致するときだけ束縛する（secret の配置と setup の前に呼ぶ契約）。束縛（`PinnedWorkspace`）は process 内の値で、supervisor の process でしか作れない。belt は agent が secret bundle の配置と prepared setup の複製を済ませてから supervisor を起動するので、secret_files を持つ source・prepared setup・fast-worktree の `[[clone]]` で依存を複製する worktree では束縛できない。この変更では、secret bundle・prepared setup・in-place を伴う CI task を受理の時点で断り、束縛できない workspace は step を起動せずに failed にする。決定（2026-11-17、親）: local-ci の側に、宣言された上書きを除いて照合する束縛を足す。belt の側で順序を変える回避はしない。その API が出たら、受理の拒否を外して束縛を差し替える。
- Git worktree の source のテストは、fast-worktree の代わりに Git だけの代役（依存を複製しない）で workspace を作っている。実物の fast-worktree が `[[clone]]` や `post_add` で workspace に何かを足した場合は束縛できず、テストもしていない。
- core を止められるのは、core が cancel handle を見る箇所だけである。`verify_workspace` の途中など handle を見ない区間は止まらず、supervisor は core の thread が戻るのを待つ。取消要求の永続化から handle の cancel までは最大 `poll_ms` かかり、その間に core が補助 process（git など）を起動しうる（step は database の claim が断るので起動しない）。
- `timeout-minutes` が切れた step に `continue-on-error: true` が付いていても、local-ci 0.13.0 の core は job を失敗として扱う（GitHub Actions とは違う）。belt は step の結果を返すだけで、この扱いは変えていない。
- step を node として登録する間（履歴の key の計算と task-traffic-controller への登録。admission の最初の look の前）は、exec の command と同じく lease を見ない。負荷の高い機械では、この区間が 1 秒を超えることがあった（lease 1.5 秒のテストで観測）。既定の lease（60 秒）では問題にならないが、step ごとに起きる。
- 資源待ちの timeout（`resource_wait_timeout_ms`）は plan の `timeout-minutes` ではなく attempt の予算なので、attempt を終わらせる扱いにしている。
- step の script を pipe から読む step（GitHub token を持つ script）は、task-traffic-controller の起動経路で fd を渡せないので実行できず、その step は error になる。
- matrix の variant ごとの workspace（`PinnedWorkspaceProvider::fork`）は `RefusePinnedForks` で断る（(2) の区切り）。portool と service container の同時実行数は local-ci の既定に任せていない（`portool: None`・`containers: 1`）。
- supervisor が再起動した CI task は、既存の recover の経路で終わる（step の続きからは再開しない）。
- forecast の `step-N` の数は plan から数える。`if:` で飛ばされた step が有ると、実際の番号とずれる（余った demand は attempt の終了まで残る）。forecast の要求のたびに、未完了の CI task の plan を読み直す（cache は無い）。
- inputs の上限 4 MiB は、接続の frame の上限（2 MiB）より大きい。今の wire では、2 MiB を超える inputs は frame で断られる。
- テストしていないもの: step の停止を確認できなかった場合の end to end（`ends_attempt` の単体だけ）、core が handle を見ない区間で止まらないこと。

残り（この時点のもの。配送と親の側は次の節で実装した。`services:` は残っている）:

- FrozenPlan の bytes の配送と、feature `ci_task_v1` の提供。worker は `<state_dir>/ci-plans/<sha256>.json` に置かれた bytes を使う（`worker::ci_task::store_plan`）。`blob_put` の経路に plan 用の種類を足して親から届けるところは未実装で、届いていない plan の attempt は準備で失敗する（`NOT_FOUND`）。配送を入れる変更で `ci_task_v1` を `SUPPORTED_FEATURES` に入れる。
- `services:` を持つ task の実行。今は受理で断る。supervisor が失われたとき、core が起動した container と compose project を recover で止める実装（core の `CancelHandle` が持つ名前を attempt に記録し、recover で止める）を入れてから、拒否を外す。
- 親の側の回収: `ci_task` を持つ payload では、結果の artifact `belt-ci-task-result.json` を必須（required）として扱う（`results/collect.rs` の `declared_of` は今 `payload.artifacts` だけを見る。worker は `ArtifactDecl` でこの名前を宣言できない）。宣言が 1024 件のとき、結果を足すと 1025 件になる点も合わせて決める。偽の worker がこの artifact だけを返すテストを付ける。
- 親の側: `ci` の実行（`src/cli.rs` と `tests/it/not_implemented.rs` の 2 行）、`TaskSpec` への `ci_task` の追加と `build_payload`、`ci_task_v1` の無い worker の除外と placement への理由の記録、Driver と同じ形の trait の後ろに置く run の進行（needs の DAG、上流の成功後だけ下流を配置、失敗時の skipped、cancel）、needs output を下流の `inputs` へ入れること（複数の上流の合計が上限を超える場合の扱いを含む）、`plan_task_demand` から `WorkloadPlan` を作って配置時の forecast に渡すこと（A10 の S16 分）、A38 のうち結果と output の分のテスト。

### S24 Linux の systemd user service と ephemeral worker（2026-11-17、worker 側の 1 本目）

作ったもの。
- `src/service/systemd/`: Linux の service backend。`policy.rs`（純粋: unit の描画、`systemctl` の引数列をデータで返す `steps`、`systemctl show` の厳密な読み取り、`systemctl --user` に渡す環境）と `SystemdLauncher`（command runner を差し込める。実 `systemctl` は `SystemctlRunner` だけ）。`policy.rs` は `scripts/pure-modules.txt` に載せた（mutation・100% 行 coverage の対象）。
- 既存の worker-host helper（`worker_service`、upgrade・rollback・commit を含む）は分けずに、`Launcher::backend()`（既定は launchd）と `Paths::resolve_for(env, backend)` で manager を選ぶ。managed file の置き場は launchd が `~/Library/LaunchAgents`、systemd が `$XDG_CONFIG_HOME/systemd/user`。journal・receipt の形、version 別 directory、upgrade の順序は変えていない。`HostLauncher::native()` が OS（`cfg!(target_os = "linux")`。両方の枝が全 OS で型検査される）で manager を選び、`worker-service` と doctor の host 側が使う。
- 起動は `daemon-reload`、`enable`、`start` の順、停止は worker の ack の後の `stop` だけ（unit は enable のまま。launchd の bootout 後に次回 login で読み直されるのと同じ）。`daemon-reload` と `enable` の失敗は NotApplied、最後の `start`・`stop` の失敗と runner の無応答は OutcomeUnknown。`show` の読み取りは `LoadState`・`ActiveState`・`FragmentPath`・`MainPID`・`NeedDaemonReload` だけを受け、他は Unknown にする。
- `worker --ephemeral --cluster <id> --name <worker> --key-file <abs path>`（`src/service/worker/ephemeral.rs`、純粋な判断は `ephemeral/plan.rs`）。service を登録せず、前景で動く。正準の worker state（`$HOME/.belt`、`worker --stdio` の橋が探す場所）が既にあるか、常駐 worker の journal があるときは、何も作らずに拒否する（常駐にならない）。起動ごとに新しい identity: worker 名は親が登録した名前のまま、報告する boot identity が `<OS の boot id>.<起動ごとの ULID>` になる。終了（SIGTERM・interrupt）時は自分が作った state directory（dev・ino を確かめる）を、worker DB に live な attempt が無いときだけ消す。使い方は `docs/linux-and-ephemeral-workers.md`。

試験。systemd 用の偽 Launcher で install・start・stop・start と upgrade の順序（`start`、`stop`、`start`）、unit の描画、`XDG_CONFIG_HOME` の置き場を macOS 上でも確かめる。実バイナリの統合試験（`tests/it/worker_ephemeral.rs`）で、ephemeral が何も導入せず、起動ごとに identity が違い、停止後に state が消え、既存 state と常駐 journal を拒否し、引数が足りなければ exit 2 になることを確かめる。`tests/it/not_implemented.rs` から `worker --ephemeral` を外した（`workers forget` は未実装のまま）。

S24 の残りは、次の「S24 親の側」の末尾にまとめた（worker 側の 1 本目の時点で挙げた 3〜5 は親の側で実装した）。

### S24 親の側（2026-11-17、2 本目）

worker 側（上の節）に対する親の側を実装した。決めたことは 4 つ。

1. **OS・arch の違う worker へ送る binary**。`workers add --install` と `workers upgrade` に `--binary <path>` を足した。指定が無いとき、親は自分の binary を、worker の OS と arch が親と同じときだけ送る。違うときは何も送らずに断り、必要な target triple（例 `x86_64-unknown-linux-gnu`）と `--binary` を message に書く。release の成果物を自動で取ってくることはしない（S26 で足してよい）。判断は純粋関数（`src/service/workers/target.rs`: `uname` と Rust の名前の対応、triple、probe の読み取り、version の一致）。「macOS の親と worker だけ」という制限は外した（Linux の親は同じ arch の Linux へ自分の binary を送れる）。
   - 切り替える前の確認: staging の直後、install・switch の要求より前に、送った binary の `--version` を worker の上で実行し、親が出す文字列（clap の `render_version`）と同じであることを確かめる。違う・実行できないときは staging の copy を消して断る（この時点で worker の導入済みの版には何も触れていない）。ssh 自体の失敗（exit 255）は copy を消さずに `COMMUNICATION_UNKNOWN`。切り替えた後の hello（protocol と feature）の確認と rollback は S23 のまま。
   - `workers upgrade` が動いている resident と比べる digest は、送る binary ではなく親自身の binary のもの（`--binary` の build は resident の build と一致しないため）。
   - staging と probe の shell は、`sha256sum`（Linux）が在ればそれを、無ければ `shasum -a 256`（macOS）を使う。
2. **Linux での `init`**。service backend 以外に妨げは見つからなかったので、拒否を外した。`init` は `HostLauncher::native()`（Linux は systemd user、他は launchd）を使い、unit を `$XDG_CONFIG_HOME/systemd/user/` に置く。init の journal（`init.json`）に、どの service manager のために書いたかを持たせた（`systemd: true`。launchd のときは書かないので、macOS の journal は今までと同じ bytes）。managed service を確かめる側（daemon・CLI）は、動いている機械の OS ではなく journal に従って managed file を探す。別の manager のための journal は引き継がない（`another service manager`）。
3. **一時 worker の親の側**。
   - `ephemeral = true` の worker を接続する（`workers add <name> --ssh <dest> --ephemeral` で登録できる）。install・upgrade・start・stop・drain・resume は断り、ssh も実行しない。`doctor` は bridge の hello だけを見る（service・version・host の事実は見ない。答えないときは notice）。
   - identity の記録（coordinator DB の step 12: `worker_identity`・`worker_identity_tombstone`）。hello の boot identity（`<OS の boot id>.<起動ごとの ULID>`）を、attach の前に判定する（純粋関数 `coordinator::identity::admit`）: 初めて・同じ → 通す。別の identity（新しい起動）→ 前の identity を退役させ、それが持っていた attempt を同じ commit で `interrupted` に確定してから通す。退役済み → attach しない。
   - 「別 boot の attempt は interrupted」という既存の規則は worker の側（`worker/policy.rs`）にあり、worker が自分の DB から確定して親に伝える。一時 worker は毎回 state が空なので、この規則は働かない（新しい起動は `not_found` と答え、未 ack の attempt は再送されてしまう）。そこで親の側に同じ意味の規則を置いた: 同じ名前で新しい identity が接続したことを、前の起動が置き換えられた証拠として扱う。新しい起動へ前の起動の attempt を送ることは無い。
   - 到達不能の間は、attempt は unknown のまま保持する（既存の `KeepUnknown`。別の worker に空きがあっても配置し直さない）。
   - `workers forget <name>`: resident への要求（`ForgetWorker`。CLI は DB を書かない）。登録が ephemeral で、かつ接続していないときだけ通る（`may_forget`）。identity を退役（tombstone は cluster の寿命の間消せない）、その worker の live な attempt を `interrupted`（cancel 済みなら `cancelled`）に確定、demand を閉じ、待っていた retire を確定、回収を `gave_up` で閉じる（消えた worker からは回収できないので、run が回収待ちで止まらない）。1 つの transaction。もう一度実行しても何も変わらない。再実行は自動ではなく、明示の retry による。
   - 退役した identity が遅れて戻ったとき、親は attach しないので、submit も renew も送らない。worker 側の attempt は、renew が来ないことで既存の lease の規則どおり止まる。
4. **doctor の linger**。systemd user service の host は `loginctl show-user <uid> --property=Linger` を service 環境で実行し、事実（`HostFacts.linger`）を返す。off は notice（警告）で、直す command（`loginctl enable-linger <uid>`）を detail に書く。答えが無いときは `not_checked`（pass にしない）。launchd の host は聞かれない。親自身は、`init` が systemd に登録した service を持つときだけ聞く。純粋部分は `src/service/doctor/linger.rs`、command の実行は既存の `doctor::Runner` の後ろ（試験は偽の runner）。

`scripts/pure-modules.txt` に足したもの: `src/coordinator/identity.rs`・`src/service/workers/target.rs`・`src/service/doctor/linger.rs`。

試験（どれも実 `systemctl`・実 `ssh`・実 `loginctl` を使わない）:
- 1: `src/service/workers/tests.rs`（`the_own_binary_is_not_sent_to_a_host_of_another_platform`・`a_named_build_is_what_is_sent_to_a_host_of_another_platform`・`a_build_that_does_not_report_the_parents_version_is_removed_before_any_install`・`a_linux_parent_sends_its_own_binary_to_a_linux_host_of_its_architecture`）、`src/service/workers/upgrade/tests.rs`（`a_worker_of_another_platform_is_upgraded_with_the_named_build`・`a_build_of_another_version_is_removed_and_the_worker_is_not_switched`）。
- 2: `src/service/init/tests.rs`（`init_with_a_systemd_manager_writes_a_user_unit_and_no_launch_agent`・`a_launchd_journal_does_not_say_systemd_and_reads_as_before`）、`tests/it/init.rs`（`init_with_a_systemd_manager_registers_one_real_combined_resident`: 偽の manager が持つ子 process で init の全体。Linux だけ: `linux_public_init_reaches_the_systemd_backend_and_claims_nothing_without_a_manager`）。
- 3: `src/coordinator/forget.rs` の試験（A46 の親の半分）、`src/service/resident/remote/tests.rs`（resident 経由の forget と admission）、`src/service/workers/tests.rs`（`an_ephemeral_worker_is_registered_and_never_installed_or_controlled`・`forget_reports_only_what_the_resident_confirmed`）、`src/service/doctor/tests.rs`（ephemeral の 2 本）。
- 4: `src/service/doctor/linger.rs` と `src/service/doctor/tests.rs`（linger の 4 本）。

S24 の残り（必須。この変更でも S24 は完了しない）:
1. 実 systemd・実 Linux 機での受入（A45）は未実施（S26）。`systemctl`・`loginctl` は偽の runner だけで試験した。実 `systemctl --user` の出力（`show` の書式、`enable` の副作用）、Linux での `init`（coordinator の unit の起動と health）、linger の実際の答えは実機で確かめる必要がある。Linux の `init` を実 binary で通す試験は、manager に届かない runtime directory で「未実装ではなく、成功も主張しない」ことだけを確かめている。
2. system-user mode（`service_mode = "system-user"`、systemd の system service を非 root の `User=`）は unit の描画だけを純粋関数として持つ。登録先（`/etc/systemd/system`）と管理権限の扱いは未実装（macOS の LaunchDaemon と同じ未了）。
3. 違う platform の worker 用の binary を用意する手段は利用者に任せている（`--binary`）。release の成果物から取ってくることは S26 で決める。`doctor` の `version` は digest の比較なので、違う platform の worker は常に「別の digest」と出る（detail にその旨を書いた。版の文字列での比較は未実装）。
4. トンネル越しの実 SSH で一時 worker に配置し、切断し、forget するまでの通し（A46 の残り、S26）。トンネルの提供は仕様どおり範囲外。
5. 退役した identity の worker へ「cleanup せよ」と伝える wire の message は足していない。親が attach しないことと、worker 側の lease 切れで止まることに任せている。
6. ephemeral が消す state は worker DB の live attempt を見て判断する。attempt の supervisor が agent の終了後も生きている場合の扱い（残す）は試験していない（worker 側の 1 本目から持ち越し）。
8. `forget`（と新しい起動による退役）が閉じる回収は、その時点で live だった attempt のものだけ。worker が消える前に terminal になり、まだ回収していなかった attempt の回収は、既存の断念の規則（保持期間）で閉じるまで待ちのまま残る。
9. forget で `interrupted` に確定した task が明示の retry を受け付けることは、既存の retry の規則（terminal の attempt）に任せており、forget からの通しの試験は無い。
10. 一時 worker は `doctor` の環境の比較（`workers diff` 相当）の対象外（比較の報告は導入済みの worker の helper が出すため）。`workers diff` / `sync` を一時 worker に対して実行した場合の扱いは決めていない。
11. binary の version は crate の version（今は `0.0.0`）で比べるので、0.1.0 より前は `--version` の確認がどの build でも通る。
7. `workers`（一覧）は未実装のまま。一時 worker の接続状態と記録された identity を見る command は無い（`doctor` の bridge の check で分かる）。（2026-11-23 追記: `belt workers` の一覧を実装した。形式は `id  ssh  ephemeral|persistent  connection  identity|-`。済み。）

### S16 (1) の後半: 親の側の `ci` の実行（2026-11-17、main `b673327` 起点）

設計の決定 (a)・(b) の親の側と、FrozenPlan の配送を実装した。`ci`（`plan` 以外）は `NOT_IMPLEMENTED` ではなくなった。matrix・中間 artifact・`services:` は含めていない。

実装したもの:

- FrozenPlan の配送: wire の `SourceBlobKind::Plan`。親は run の source を worker に届けたあと、同じ gate の中で plan の bytes を `blob_put`（64 KiB ずつ）で送り、それから入力を ready として公開する（`service::forward::plan`、`resident::delivery`）。worker は source の受け口とは別に受け取り（`worker::ci_task::put_plan`。source が公開済みかどうかに依らない）、全 bytes が届いて digest が合うときだけ `<state_dir>/ci-plans/` に置く。途中の file は 8 件まで。既に持つ plan をもう一度送られたら完了と答え、置かれた時刻を更新する（GC の保持期間が延びる）。
- feature `ci_task_v1` を `SUPPORTED_FEATURES` に入れた（required には入れない）。交渉していない worker には CI task を配置せず、除外の理由（`protocol feature ci_task_v1`）を placement の decision に残す（`Exclusion::CapabilityMismatch` と `unmet`。全員がそうなら `task_waiting` の `blocked_reason` は `no_compatible_worker`）。plan も送らない。
- `TaskSpec.ci`（`CiSpec { task, hold, steps }`）。`build_payload` が `ci_task` を作る。CI task は `commands` を持たない。提出のとき、worker が断るもの（`worker::ci_task::check_payload`）と、宣言が結果の 1 件ぶんの余地を残さないものを断る。
- 親の側の `ci`（`service::ci_run`）: capture 1 回、`service::planning::freeze`（`ci plan` と同じ関数）で FrozenPlan を 1 回作り、bytes を親の `<state_dir>/ci-plans/` に置く。plan の task をすべて 1 つの run の task として `SubmitCaptured` で提出する。task の ID はここで決まる。needs を持つ task は `hold` 付きで提出する。resident への要求の schema_version は 5（CI task を知らない resident は断る）。提出のあとは `exec` と同じ待ち方をし、Ctrl-C で run を cancel する。終了コードは run のもの。JSON と text に Job ごとの結果（`success`・`failure`・`skipped` と理由・`cancelled`、結果を残さなかった task は `no_result`）を出す。
- 待機: `hold` の task は run の task の行として存在するが、queue の述語（`QUEUED`）から外れるので配置されない。run の完了（`Snapshot`）・cancel・`ls`・`show` は既存の経路のまま待機中の task を数える。schema の変更は無い（task の spec の JSON で表す）。
- 進行（`ci::progress` と `ci::advance`、resident の `ci_runs`）: `Progression` trait（`step(Input)`・`pending_actions()`・`outcome()`。入力は `Advance`・`Cancel`・`Settle`・`Fence`・`Ack`、action は `Ready { task, inputs }`・`Finished`）の後ろに `NeedsProgression` を置いた。local-ci の shared Driver が公開されたら、この trait の実装を差し替える。resident は heartbeat ごとに、待機中の task を持つ run について、run の task の行・plan の file・回収済みの結果から進行を組み立て直し、`Ready` になった task に inputs を書いて queue に入れる（`release_ci_task`。cancel の確認と同じ transaction）。process の中に状態を持たないので、再起動しても続きから進み、task を二度作らない。
- skipped・`always()`・`failure()`: 親は Job を実行するかどうかを決めない。上流がすべて終わったら（成功・失敗・skipped を問わず）下流を配置し、worker の core が inputs から `if:` を評価する。実行しない Job は step を起動せずに `skipped` の結果を返し、attempt は成功で終わる。
- 結果の artifact を required にした（`results::collect::declared_of`）。無いときは `required_missing`。有っても、その task の結果として読めないとき（JSON でない、別の task のもの、digest が合わない、16 MiB 超）は、回収を閉じるときに `required_missing` にする（`Collector::judged`）。どちらも command が成功していても task は成功にならず、下流の inputs にはその Job が `failure` として入る。
- forecast（A10 の S16 分）: 提出のとき `plan_task_demand` の node の数を `CiSpec.steps` に入れる。配置時の forecast の質問は `step-0`〜`step-(N-1)` の node（前の step に依存）を持つ `WorkloadPlan` になる（`scheduler::ci_workload_of`）。placement の demand にも `ci_steps` を残し、未 ACK の placement を次の質問に載せるとき（`pending_plans`）も同じ node になる。
- 親の plan の片付け: 待機中の task が参照せず、結果の保持日数（最短 1 日）を過ぎた plan を消す。
- live の log の polling は CI task では行わない（step ごとに node が違う）。終了後の回収は既存のまま。

自分で決めたこと（仕様に無かった点）:

- 親の側では skip しない。引き継ぎメモは「失敗なら下流は skipped」としていたが、`if:` の評価（`success_gated`）は local-ci 0.13.0 では公開されておらず、親で真似ると core と食い違いうる。下流を inputs 付きで配置し、core に決めさせる。そのぶん、skip される Job にも配置と workspace の準備が 1 回かかる。Driver に置き換えると core が配置前に決める。
- 進行の入力は、attempt の終了ではなく回収が閉じたことにした（結果が親に来る前に下流は作れない）。
- 使える結果を残さなかった task の Job は、下流には `failure`（attempt が cancelled、または配置前に cancel されたものは `cancelled`）として渡す。
- inputs の上限は 1 MiB（worker の上限 4 MiB、接続の frame 2 MiB より小さく）。超えた task は event `ci_task_unrunnable` を残して cancel する（run は成功にならない）。
- 宣言が 1024 件のとき結果を足すと 1025 件になる点: CI task が宣言できる artifact と output は合わせて 1023 件まで。`ci` は今、artifact も output も宣言しない。
- 除外の理由は新しい `Exclusion` を足さず、`CapabilityMismatch` と `unmet` で表した。 plan を受け取らなかった worker だけは別で、`CapabilityMismatch` にすると全員がそうのとき `no_compatible_worker` になってしまう（一時的な理由なのに）ので、`Exclusion::InputRefused` を足した。
- 提出で断るもの: matrix の variant・式の matrix・`services:` を持つ plan、secret_files を持つ source、task が 1 つも無い plan。64 個を超える task は resident が断る（既存の上限）。 split 済みの Shard は S18 から受け付ける（plan の ShardGroup と形が合わないものだけを断る）。
- `ci` は toolchain の凍結（`exec` の `freeze_toolchains`）をしない。profile と requires は `exec` と同じく task に入れる。（2026-11-23 追記: PR #124 で `ci` の task も提出時に toolchain を固定するようになった。済み。）

テスト:

- 実 binary・resident・local worker・実際の workflow file・実 process（`tests/it/bootstrap.rs`）: Git の repository の worktree（未 commit の変更込み）と、`--snapshot commit --ref HEAD` の両方で needs の連鎖が通ること（worker は実物の fast-worktree で workspace を作る）。 3 段の needs の連鎖で output が 2 段先まで渡り、各 Job が 1 回だけ実行されること。失敗した Job の下流が step を起動せず skipped になり（その先も）、`if: always()` と `if: failure()` は動き、`if: success()` は動かず、run が失敗になること。Ctrl-C で実行中の Job が止まり、待機中の Job（`if: always()` 付き）が配置されないこと。
- A38（`tests/it/collection.rs`、worker は memory 上の代役 2 台、plan と回収は実物）: 下流は上流の結果が回収されるまで配置されず、回収後に別の worker へ output 付きで配置され、何度見直しても attempt は 1 つずつ。`ci_task_v1` の無い worker には配置されず、理由が decision に残る。forecast の demand が step の数ぶん有る（A10）。結果が無い・読めない task は成功にならず、下流に `failure` が渡る。結果だけを返す worker は回収される。cancel した run は何も release しない。
- 単体: `ci::progress` 5 件、`ci::advance` 4 件、`service::ci_run` 3 件、`worker::ci_task` の配送 4 件、coordinator 4 件、ほか。worker への配送は `tests/it/worker_agent.rs` で実接続（追加の offer 無し）を通している。

制約と残り:

- A38 の「別 host」は、代役の worker 2 台で確かめた。実 process の worker 2 台での確認は S25（1 台上の試験ハーネス）に残る。artifact の分（設計 (c)）は未実装。
- matrix の展開・permit・max-parallel・fail-fast・join・集約と A27（(2) の区切り）は Driver の公開待ち。今は提出で断る。
- `services:` を持つ task（worker の回復で container を止める実装が先）。secret_files・prepared setup・fast-worktree の `[[clone]]` を伴う CI task（local-ci の束縛の API 待ち）。
- `retry` で上流の task をやり直しても、すでに release された下流の inputs は変わらない。下流を作り直す経路は無い。
- 親の plan は、release 済みで未配置の task が参照していても、保持日数を過ぎれば消える（run が保持日数より長く止まっている場合だけ）。worker の plan も、届いてから保持期間（既定 7 日）を過ぎて配置された task は準備で `NOT_FOUND` になる。入力の gate が run を忘れたあとの再配送では、もう一度送られて保持期間が延びる。
- `ci` の終了コードは最初に失敗した task のもの（`exec` と同じ計算）で、Job の失敗は attempt の exit 1 として現れる。
- CI task の実行中の log は `logs --follow` に流れない（終了後に回収される）。
- `ci` は、`exec` が project の設定から入れるもののうち、toolchain の凍結・`prepare_commands`（2026-11-23 追記: toolchain の凍結は PR #124 で入った。） と fast-worktree の setup（prepared setup は CI task で断られる）・`outputs` の宣言・`pass_env` を task に入れない。
- `estimate`（`coordinator/estimate.rs`）は workload を `commands` から作るままで、CI task の step を見ない。（2026-11-23 追記: 保存済みの CI task の `estimate --task` は、配置と同じ `workload_of_spec` で step ごとの node を作る。済み。）
- host の loop（`ci::advance`）は `NeedsProgression` の `is_settled`・`dependencies_settled` を trait の外で呼んでいる。Driver に差し替えるときは host の loop も直す。
- run が受理される前に記録された cancel（`run_cancel_intent` だけが有る状態）と待機中の task の release の関係は、command の task と同じ経路（配置の側の扱い）に任せていて、CI 用のテストは無い。
- mutation のテストは、変更した pure module（`coordinator/policy.rs`・`coordinator/input_gate.rs`・`scheduler/decide.rs`）だけを手元で回した。`advance_run`・`ended`・`release`・`job_lines`・`Collector::judged` は mutation の対象（`scripts/pure-modules.txt`）に入っていない file に有り、`tests/it` と lib のテストで確かめている。

### S16 (1) の後半の追補: 黙って待つ 2 つの場合を無くした（2026-11-17、PR #70）

- 進められない run: `ci::advance` の失敗を `Fault::Unrunnable`（親の持つ状態から決まる。plan の file が無い・digest が違う・読めない、task が plan と対応しない、進行の規則が拒む）と `Fault::Transient`（store の error、plan の file のその他の読み取り失敗）に分けた。前者は `CoordinatorStore::fail_ci_tasks` で、その run のまだ配置されていない CI task を理由付きで cancel する（spec の `ci.unrunnable`、event `ci_task_unrunnable`、cancel を 1 つの transaction で）。cancel 後は `ci_waiting_runs` に出ないので、同じ run を二度扱わない。inputs が上限を超えた task の cancel も同じ関数に寄せた（以前は event と cancel が別の書き込みだった）。後者はやり直し、resident が event `ci_run_stalled` を障害ごとに 1 回残す。
- 配送の側（`resident::delivery`）でも親が plan を読めなければ同じ `fail_ci_tasks` を呼ぶ。待機中の task を持たない run（Job が 1 つだけ等）は進行の pass に現れないため。
- 表示: `show`・`ls` の task の `unrunnable`、`logs` の attempt を持たない task の status（`reason` と `placement`。follow 中に内容が変わったら出し直す）、`ci` の Job の行の `reason`。
- plan を受け取らない worker: `forward::plan` の失敗を、worker の答え（`Refused`）・親が読めない（`Unrunnable`）・その他（`Retry`）に分けた。`Refused` は接続の `InputGate` に run ごとの理由として持ち（64 件まで、受け取ったら消える、worker が別の boot / epoch になったら消える）、run に event `ci_plan_refused` を 1 回残す。配置はその worker を `Exclusion::InputRefused`（理由は `unmet`）で外す。tick が見る run（どこかで ready な run）に、断られた run も加えた。そうしないと、どの worker でも ready でない run は `place_one` に届かず、`task_waiting` が残らない。
- 自分で決めたこと: (a) は run 全体の cancel ではなく、未配置の task だけを cancel する（実行中の Job は結果を残させる。plan が無くても worker は自分の写しで実行できる）。(b) は fail にせず除外にした（worker の GC で空けば解ける一時的な状態で、ほかの worker が取れる）。全 worker が断っている間は、理由付きで待つ。待つ時間に上限は設けていない。
- テスト: lib（`coordinator::tests` 2 件、`input_gate` 2 件、`policy` 4 件、`ci_runs` 2 件、`advance` の分類）。`tests/it/collection.rs` 2 件（plan が消えた run、plan を受け取らない代役の worker）。`tests/it/bootstrap.rs` 1 件（実 binary・resident・local worker で、worker の `ci-plans/` を 256 件で埋めると `ci` が理由を残して待ち、空けると完了する）。
- 残り: 配送の側で親が plan を読めない場合（待機中の task を持たない run）は、分類の単体テストだけで、実 binary のテストは無い。`ci_run_stalled` は event に残すだけで、`show` には出していない（DB が使えない間は event も書けない）。すでに worker に配置された attempt が plan を待っている場合（cold placement）の扱いは変えていない。`fail_ci_tasks` が cancel するのは attempt を 1 つも持たない task だけなので、`retry` で queue に戻った task（retire 済みの attempt を持つ）は、親の plan が消えていても cancel されず、配送が失敗し続ける（event も出ない）。（2026-11-23 追記: `fail_ci_tasks` は retire 済みの attempt だけを持つ task も対象にするようになった。済み（R20）。）どの worker でも ready でなく、1 台が plan を断っている run では、転送時間の実測がある worker に prefetch の hint が出るようになった（以前は何もせず戻っていた。配置はしない）。

### S18: sharding の配置（2026-11-17、main `979fec2` 起点）

FrozenPlan が split 済みの ShardGroup を持つとき、親がその Shard を配置し、集約し、fail-fast で止め、失敗した Shard だけを retry できるようにした。local-ci は 0.13.0 のままで、変更していない。

- 判断は pure module `src/ci/shards.rs` に置いた（`scripts/pure-modules.txt` に登録）。Shard と ShardGroup の対応（`group_of`）、親が配置できる形かの検査（`shape_fault`）、receipt の本物判定（task と Job の名前が一致するものだけ。`receipt`）、Shard の失敗（`shard_failed`）、fail-fast で止める Shard（`to_cancel`）、Shard の outcome から Job の outcome を作る規則（`combine`・`conflicting_outputs`・`join`）、問題の要約（512 文字まで。`diagnostic`）。規則は local-ci の共有 Driver（0.18 の `driver/shards.rs`）と同じで、test の集合の検査は `local_ci::complete::aggregate_shard_group` を呼ぶ。`combine` と `conflicting_outputs` は 0.13.0 で非公開のため、同じ規則を写した。
- 配置: `ci::progress::unsupported` は Shard を断らなくなり、形が plan と合わないものだけを断る。Shard は依存を持たないので、提出の時点で全部が queue に入り、同じ tick で並んで配置される。
- 集約: `NeedsProgression` は、ShardGroup の全 Shard が終わったときに Job の outcome を 1 つ作る。後続の task の inputs と `outcome()` には、Shard ごとの outcome ではなくこの 1 つだけを出す。全 Shard が成功でも、receipt の無い Shard・報告されなかった test・二重に報告された test・Shard 間で値の違う output があれば Job は failure になり、理由を持つ。
- fail-fast: ShardGroup が `fail_fast` で、終わった Shard に失敗が有ると、進行は終わっていない Shard に `ActionKind::Cancel` を出す。host（`ci::advance`）は、未配置なら `fail_ci_tasks`（理由を `ci.unrunnable` に書いて cancel）、配置済みなら `cancel_task`（coordinator が tick で worker に届ける）で止める。cancel 要求済みの行には何もしないので、pass を繰り返しても 1 回だけになる。失敗した Shard の結果は残る。
- pass が見る run: `ci_waiting_runs` は、待機中の task を持つ run に加えて、まだ集約されていない split Job の Shard（`CiSpec.shard_group` が有り `joined` が無い）を持つ run を返す。cancel 済みの Shard も数える（cancel された run の Job も集約して run を確定させるため）。後続の Job を持たない split Job でも fail-fast と集約が働く。
- retry: run の retry は成功した task を飛ばすので、失敗した Shard と止められた Shard だけが同じ FrozenPlan の新しい attempt になり、通過した Shard は attempt も結果もそのまま残る。S15 の規則を 1 点だけ広げた: fail-fast で一度も配置されないまま cancel された Shard（attempt が無く、cancel 済みで、`ci.unrunnable` が fail-fast の理由のもの）は retry の対象になり、cancel と理由を外して queue に戻る。attempt を持たないそれ以外の task は従来どおり断る。Shard を retry すると、その ShardGroup の全 Shard から `joined`・`join_failure` を外し、全部が終わったら残した結果と新しい結果から Job の outcome を作り直す。 attempt が成功でも「単独で通過」と言えない Shard はやり直す: Job が failure で集約されたとき、receipt が無い・test が失敗した・渡された test をちょうど 1 回ずつ報告していない Shard に `ci.rerun` を付け（全 Shard が単独では通過していて集約だけが failure のときは全 Shard。`ci::shards::verified`・`rerun`）、run の retry はこれを成功扱いにしない。集約の書き込み（`join_ci_shards`）は、同じ transaction の中で全 Shard が終了していることを確かめ、読んだ後に retry が入った場合は何も書かない。
- 報告: `ci` の Job の行に `part` を足した。Shard は `shard`（名前は plan の task id）、split Job は全 Shard が終わった後に `job` の行が 1 つ付き、failure のときは `reason` に問題の要約が入る。
- run の結果: split Job の outcome は、進行の pass が全 Shard の終了を見た時点で Shard の行に書く（`CoordinatorStore::join_ci_shards`。`ci.joined`、failure なら `ci.join_failure` に理由、event `ci_job_joined`）。run の結果（`CoordinatorStore::completion`）はこれを読む。集約前の Shard は「回収待ち」と同じ扱いで run は確定せず、集約が failure なら attempt が全部成功でも run は失敗（exit code 1。attempt 自体の失敗が有ればその exit code が先）になる。`ci`・`wait`・`show`・JSON・status（S19）は同じ run の結果を読むので、どこでも同じになる。task 単位の結果（`wait <task>`）は Shard 自身の結果のままである。進められない run（plan が消えた等）は、未集約の split Job を理由付きの failure として閉じる（`fail_ci_joins`）。 run が何で失敗したか（exit code）は、fail-fast で止められただけの Shard ではなく実際に失敗した task のものにする（run 全体の cancel が無いとき。task id の順に依らない）。attempt は成功で test だけが失敗した Shard が原因のときは Job の失敗（exit code 1）になる。
- 分割できない Job（A41）: 分割の要求が断られた plan は元の task 1 件のままで、ほかの Job と同じ経路で動く。理由は `ci plan` の `shard_groups[].not_split` と text の `is not split: [...]` に出る（S16 で実装済み）。

自分で決めたこと:

- Job の outcome は Shard の順（plan の `shards` の順）で作る。到着順は使わない。
- receipt の名前が合わない結果は、進行を止める誤りにせず「receipt 無し」として扱う（Job は failure になり、`missing: shard …` と出る）。
- fail-fast の「失敗」は、Shard の Job が failure か、receipt に失敗した test が有るか exit が失敗のとき。cancel された Shard と skipped は失敗に数えない。
- receipt に失敗した test が有れば、Shard の step が成功で、ほかの Shard が止められて cancelled でも、Job は failure にする（共有 Driver は success のときだけ failure に変える。fail-fast で止めた結果が「cancelled」になり失敗が隠れるため、ここは変えた）。
- split Job の行の `reason` は Driver と同じ要約を出すが、failure のときは Shard 個別の skip 理由（`skipped`）を出さない。

テスト: `tests/it/collection.rs` に 8 件（A39: Vitest と Jest の file 単位の分割を worker 2 台の構成で同じ tick に配置し、全 file が 1 回ずつで Job が 1 つになること、報告されない file が有ると Job が failure になること。A40: 履歴で遅い test が 1 Shard になり最長の Shard が短くなること、履歴無しでも同じ分割になり実行できること。A41: 分割できない理由の表示と単一 Job への fallback、fail-fast の cancel の配送と停止確認と報告、後続を持たない Job で一度に配置しきれない Shard が未配置のまま止まること、retry、全 Shard が成功で集約だけが failure の run が集約まで確定せず失敗で確定すること）。失敗の原因と retry の対象は、止められた Shard の id が先に並ぶ場合、attempt が成功で test だけ失敗した場合、報告漏れの Shard だけをやり直す場合、retry の後の古い集約が書かれない場合を含む。lib は `ci::shards` 11 件、`ci::progress` 3 件、`ci_run` 1 件（ほかに既存の 2 件へ追記）。mutation は `src/ci/shards.rs` と `src/service/completion.rs` を手元で回した。

以下は 2026-11-17 の初回実装で残っていた項目。11 月 18 日の統合では、worker の workspace で runner を列挙し、結果と runner の版を固定してから分割する。時間計測・worker 間の速度差・準備費を配置に使い、非冪等な shard の retry は捕捉済みの許可を確認する。独自 reporter や後続の成果物公開を分割で再現できない場合には理由を示し、元の Job を実行する。対象試験の進捗は上の A39〜A41 の行に記録する。

初回実装時点の残件は次のとおり。

- 本番の `ci` は分割の要求（`ShardRequest`）を作らない。`service::planning::freeze` は test の列挙（adapter の discover）を走らせないので、実際の `ci` の plan は今も Shard を持たない。上のテストは `FrozenPlan::with_shards` で作った plan を使う。列挙をどこで走らせるか（親か worker か、どの workspace か）は未決定で、adapter 本体の側の作業である。
- 履歴の入力元が無い（A40）。worker が返す `TestResult` は所要時間を持たず、`History` と `PartitionOpts.speed_permille`（worker の性能補正）に入れる値を belt は記録していない。Shard 数を worker の実効容量と残りの DAG から決める処理も無い。
- worker が Shard を実際に実行する経路（`execute_pinned_task` が Shard の command を作り `ShardOutcome` を返す）は local-ci の側のテストに任せていて、belt では実 runner を動かしていない。テストの worker は receipt を手で作る。
- retry しても、すでに failure の inputs で release された後続の task はやり直さない（S16 の plain な Job と同じ）。
- 非冪等な runner の明示的な retry 許可（`ExecutionTask.retry`）は見ていない。
- coverage の merge、JUnit などの artifact の重複・欠落の検査、assertion 数の集約は無い（local-ci は coverage を集める Job を分割しない）。
- `ci plan` は split 済みの ShardGroup を JSON の `shard_groups` に出すが、text では数だけである。
- 集約は resident の進行の pass が行う。resident が止まっている間は、全 Shard が終わっていても run は確定しない（待機中の task を持つ run と同じ）。
- fail-fast で止めた実行中の Shard には理由を記録していない（未配置で止めたものだけ `ci.unrunnable` に理由が残る）。（2026-11-23 追記: PR #119 で記録するようにした。止める要求と同じ transaction で `ci.unrunnable` に理由を書き、`show <run>` に出る。retry で開き直すと消える。止める直前に成功した Shard にも理由は残る（既知の制限）。）

### S19 GitHub status（2026-11-17、main `979fec2` 起点）

- 判定は pure module `src/ci/status.rs`（`scripts/pure-modules.txt` に追加）。3 つに分けた: run の計算結果（`computed`）、投稿してよいかと宛先（`decide`）、投稿の成り行き（`outcome`）。投稿の成否は計算結果にも `ci` の終了コードにも入らない。
- 計算結果は wait の後、run の結果（`CoordinatorStore::completion`。`ci` の終了コード・`wait`・`show` と同じもの）と、親が回収した行から決める。run の結果を判定し直さない（S18 の取り込みで直した。`service::ci_run::Ended` が completion の snapshot を `ci::status::RunEnd` に写す）。
  - run が何で失敗したか（`ci::status::cause`）は、completion の `first_compute_failure` を 3 つに分ける。Job: command が 0 以外の code で終わった、または Job の失敗（Shard の test の失敗、集約が failure の split Job。attempt 自体の終わり方では説明できない失敗）。cancel: task が cancel で終わった（signal や code で止まった場合も）か、配置されないまま cancel された。その他: timeout・signal・失われた attempt・中断。
  - failure: run が Job で失敗した、または `failure` の行がある。集約が failure の run は、attempt が全部成功でも、plan が消えて Job の行を作れなくても failure になる。
  - error: timeout・signal・失われた attempt・回収の失敗など、Job の失敗ではないもの。local-ci の対応（`report::github_state`: success は success、failure は failure、それ以外は error）に合わせている。
  - cancelled（投稿しない）: run が自分で settle していない。または cancel の要求があり、run の終了コードが 0 か、run が cancel で失敗している。fail-fast が残りの Shard を止めただけの run は、失敗した task で失敗しているので cancelled にならず failure（Job の失敗でなければ error）を投稿する。Shard を止めた run 全体の cancel は completion が cancel を run の失敗にするので cancelled。何も止めなかった cancel でも、終了コード 0 の run は success にしない。
  - success は閉じた判定で、run が自分で settle した・cancel の要求が無い・回収が全部閉じた（`collection_pending == false`）・終了コード 0・提出した plan の task が 1 つ以上あり全部に行がある・行が `success` か `skipped` だけ、の全部が要る。Job の行が読めなかった場合（空）や `no_result` は success にならない。
  - cancel と回収中は最終状態ではないので投稿しない。
- 宛先は、入力を capture した commit（封をした manifest の `original_head`）だけ。その commit の tree（親の repository で `rev-parse <head>^{tree}`）と、検査した入力の tree（`worktree_tree`）が同じときだけ投稿する。dirty な worktree・未追跡の file を含む snapshot・directory・dirty な submodule（`submodules[].dirty`。superproject の tree には現れない）は、success でも failure でも投稿しない（local-ci の `publishable` と同じ門。A32）。`--snapshot commit --ref <ref>` は、worktree が dirty でもその commit に投稿する。
- 投稿は親の `ci` の process だけが行う（`src/service/github_status.rs`）。`gh api repos/<owner>/<name>/statuses/<sha>` に `state`・`context`・`description` を渡す、local-ci と同じ呼び出し。既定の context は `local-ci`（`--status-context` で変える）、`--no-status` で投稿しない。state は success / failure / error。description は `<state>: pass P / fail F / error E / skip S`。credential は親の `gh` が持つものだけで、task・plan・resident への要求のどれにも入れない。network は trait `Poster` の後ろに置き、テストは偽物（lib は closure、実 binary のテストは PATH に置いた偽の `gh`）を使う。
- 表示: `ci` の text に `status: ...` の 1 行、`--json` の `result.github_status`（`computed`、`posting` = `posted` / `skipped` / `failed`、`sha`・`state`・`context`・`reason`・`error`、`source_digest`、`original_head`）。同じ record を resident への `RecordStatus` で run の journal（`github_status` の event、attempt 無し）に残し、`show <run>` の `run.github_status` に最後の 1 件を出す。記録に失敗しても（古い resident など）warning だけで、結果は変えない。
- 自分で決めたこと: (a) 投稿するのは resident ではなく `ci` の process（wait して結果を受け取る側。credential を常駐 process に持たせない）。(b) 門は success だけでなく全 state に掛ける（local-ci と同じ。dirty の failure を元 HEAD の failure にもしない）。(c) 投稿先に出来る origin が無い repository は失敗ではなく `skipped`。(d) schema は変えない。`show` は attempt を持たない event だけを新しい順に辿る（既存の `event_attempt` index）。(e) `ls` には出さない。
- テスト: `ci::status` の単体 16 件（A32 を含む）、`storage::tests` 1 件（記録と `show` の読み出し）、`tests/it/bootstrap.rs` 1 件（A32。実 binary・resident・local worker・偽の `gh`: 追跡 file の変更と未追跡 file だけの worktree は投稿 0 回で computed は success、`--snapshot commit` は HEAD に 1 回、`--no-status` は 0 回、`gh` が失敗しても終了コード 0・`settled`・`computed: success` のままで `show` に `failed` が出る、1 回だけ試して再試行しない）、`tests/it/collection.rs` の 5 件（S18 の 3 件、CI run の cancel 1 件、結果を回収できない CI task 1 件）に計算結果の確認を足した（fail-fast の run と集約だけが failure の run は failure、plan が消えた run も failure、run 全体の cancel は cancelled、何も止めなかった cancel は failure のまま、retry で通った run は success、結果を回収できなかった run は error）。mutation は `src/ci/status.rs` だけを手元で回し missed 0。
- レビュー（PR #75）の反映:
  - `gh` は 30 秒で打ち切る（自分の子 process を handle から kill し、`failed` の `error` は `timed out`）。
  - 投稿先は GitHub の host にある origin だけ（`github.com`、`GH_HOST` が有ればその host）。それ以外の forge や local path の origin は `skipped`。
  - origin が、入力を capture したときの repository（manifest の `repository_id` = `github:<owner>/<name>`）と違えば `skipped`。
  - `gh` は PATH の絶対 path の entry からだけ探し、repository の外（親の state directory）を cwd にして実行する。
  - 「GitHub にまだ無い」と言うのは `No commit found` のときだけで、ほかの 422 は答えの 1 行目をそのまま出す。

残り:

- 後からの投稿: `ci` を待たずに終わった run（interrupt、`submit` だけ、wait の中断）や、push 前で `failed`（`... is not on GitHub yet`）になった run に、後で投稿し直す command は無い（local-ci の `local-ci status` に当たるもの）。`retry` で結果が変わった run の status も更新しない。（2026-11-23 追記: `belt ci status <run> [--post] [--status-context <name>]` を実装した（PR #132）。`--post` なしは表示だけで、`--post` は記録した結果から状態を決め直して投稿する。投稿できなければ終了コード 1。この呼び出し自体が原因の skip は run の記録に残さない。済み。）
- `logs` には status の行を出していない（`ci` の出力と `show <run>` だけ）。（2026-11-23 追記: PR #120 で `logs <run>` が、出力のあとに status の行を stderr へ出すようになった。`--json` では `kind: github_status` の record。`--follow` は run が終わったときに 1 回出す。済み。）
- description の件数は行の数で、split Job は Shard の行と Job の行の両方を数える（3 Shard の Job が成功なら pass 4）。
- Shard を持たない run で、先に失敗した task が有った後に run 全体を cancel した場合、completion の終了コードは失敗した task のものなので failure を投稿する（S18 の取り込み前は投稿しなかった）。その task が timeout や signal で終わっていれば error を投稿する。進められない run（未配置の task を理由付きで cancel したもの）は cancel で失敗した run なので cancelled のままで、投稿しない。
- status を決めるとき source store を foreground で開く。別の capture と重なって開けなければ `skipped`（`the input of the run cannot be read`）になり、投稿されない。
- `gh` の時間切れで止めるのは直接の子 process だけ。`gh` が起こした孫 process（credential helper など）は残り得る。孫が pipe を持ったまま `gh` が失敗で終わると、出力の読み取りが孫の終了まで待つ。（2026-11-23 追記: PR #122 で `gh` を自分の process group で動かし、時間切れでは group ごと止めるようにした。済み。）
- `local-ci status` 側に同じ検証を適用する件（仕様の一文）は local-ci の側に既に有り（`publishable`）、この行では local-ci を変えていない。
- tracked file の exclude や追加 input の扱いは `worktree_tree` の一致に任せている（ignored の secret_files は仕様どおり一致判定に入れない）。CI task は secret_files を伴う source をまだ断るので、その組み合わせの実 binary のテストは無い。

### WP24: グループと、run を投げたメンバー（2026-11-20）

仕様 第24章（仕様版 1.4）の実装行は、分割表の WP24a〜WP24e・R68・R96 である。採用しなかった案（Raft などの合意による構成と、メンバーごとの信頼の登録・取消）は仕様 §24.9 に理由を書いた。ここには、各行が足す DB の migration と wire の feature をまとめる。

DB の migration（既存は coordinator DB が step 1〜19、worker DB が step 1〜3。step は追記だけで、既存の step は書き換えない）:

| DB | step | 行 | 内容 |
|---|---|---|---|
| worker | 4 | WP24b | `worker_attempt`・`tombstone`・`lease`・`command_launch`・`log_spool`・`blob_ref` を作り直し、鍵を `(coordinator_id, attempt_id)` にする。task ごとの live な attempt の一意 index（今の `worker_attempt_one_live` は `task_id` だけ）を `(coordinator_id, task_id)` にする。今は主キーが `attempt_id` だけなので、作り直さないと、あるメンバーが別のメンバーの仕事を `RETIRED`・`CONFLICT` にできてしまう。trigger `tombstone_no_delete` を、届かなかった attempt の tombstone だけは消せる形に置き換える（仕様 §24.4）。`worker_state.max_coordinator_epoch`（全体で 1 つ）を `coordinator_id` ごとの表にする。グループ鍵の世代（番号と指紋、今・次）、メンバーの名前と `coordinator_id` の組、worker identity の後半（`send-key` で作り直す）、退役を告げられた identity、更新のための保留を持つ表。この step は drain 済みで、live な attempt も、終了したが result が回収されていない attempt（`state = 'terminal'` で `acked_at_ms` が空）も無いときだけ進む（未回収の result を捨てると、投げた側の task が unknown のまま残るため）。migration は SQL 文字列の一覧なので、この前提は SQL の実行前に code で検査する。どちらかがある DB で新しい binary の resident を起動したときは、両方の件数と対処手順（前の版で drain し、終了と回収を待つ）を示して起動を断る（A58）。古い attempt・tombstone・lease・epoch の行は引き継がずに捨てる（最初の公開版より前の DB なので、所有者を推測しない）。捨てた attempt の ID は同じ transaction で表 `dropped_attempt` に残し、agent が次の起動で、attempts の directory 直下にあるその名前の directory（spec・supervisor の状態・workspace の記録）だけを消してから行を消す（symlink はたどらず、同じ ID を再び持つメンバーがいれば触れない。消せなかった分は次の起動でやり直す）。workspace 自体は既存の起動時の掃除（行の無い workspace）が消す |
| coordinator | 20 | WP24b | 新しい表を 2 つ足す: 退役した worker identity へ通知を送った記録と、「削除済みメンバー」の小さな記録（名前と削除時刻。SSH 到達の警告が消えるまで残す。仕様 §24.2）。既存の `worker_identity_tombstone` は trigger（`worker_identity_tombstone_fixed`）が UPDATE を拒むので、列を足しても書けない。そのため別の表にする |
| coordinator | 21 | WP24c | `placement` に submit の送信回数の列 `submit_sends` を足す（送る直前に 1 つ進めてから送る。NULL は、この step より前に作った配置で「分からない」を表す）。「1 回だけ送り、その 1 回に答えが返った」を、親の再起動をまたいで判定するために要る（仕様 §24.4）。retire せずに閉じる理由 `capacity_refused` と event `submit_refused_capacity` は、既存の理由（`worker_cannot_run`）と同じく文字列で持ち、schema の制約は無い。容量で断られた attempt を探す index は足していない（一時的な除外は `inspect` の答えから tick ごとに決め、DB からは探さない） |

- WP24d・WP24e・R68・R96 は、今の見込みでは DB の step を足さない。drain の状態は worker 全体のもので、メンバーごとには持たない。必要と分かった行は、この表に step を足してから実装する。
- step の番号は main へ入る順に確定する。上の番号は WP24b → WP24c の順で入る前提で、順が変わったら表を直す。古い binary は知らない step を持つ DB を拒否する（既存の互換表の規則）。R68 の rollback が親 DB と worker DB を一緒に戻すのは、このためである。

hello の feature と wire の追加（3 つとも最初の公開版から必須にする。これより古い公開版は無いので、持たない相手との互換の枝は作らない。schema version は上げない）:

| feature | 行 | 足すもの |
|---|---|---|
| `group_membership_v1` | WP24b | 相互の操作 `group_proof`（要求: メンバーの名前・鍵の指紋・自分の nonce・`cluster_id`／`coordinator_id`／名前／相手の nonce に対する HMAC。答え: 相手の HMAC と、worker identity・boot・epoch・profile・能力）。hello の答えに nonce を足し、identity などを `group_proof` の答えへ移す。断る答えの理由（`group_mismatch`・`group_key_mismatch`・`member_identity_mismatch`・`identity_retired`） |
| `identity_retired_v1` | WP24b | 操作 `identity_retired`（要求: `coordinator_id`・`worker_id`・`worker_boot_id`。`worker_id` が自分の名前のときだけ受ける。同じ内容の再送に同じ答え） |
| `shared_admission_v1` | WP24c | `submit_attempt` の `OVERLOADED` に付ける `overloaded_reason.reason = "capacity"`、`inspect` の受付窓 `admission`（上限 `limit`・使用数 `used`・聞いたメンバーの分 `own`・他のメンバーの分 `foreign`。共有の有無は `foreign > 0`）、forecast の `foreign_demand`（他のメンバーの非 terminal の attempt の件数）。「自分で投げた非 terminal の run の有無」は、それを読む `workers stop`（`--all-submitters`）と一緒に WP24d で `inspect` へ足す（項目の追加なので wire の互換は変わらない） |

WP24c の実装メモ（2026-11-21）:

- worker の受付窓の判定は `WorkerStore::submit_within` の中、attempt を insert するのと同じ transaction で行う。古い epoch・tombstone・受理済み attempt の再送・`attempt_id_in_use`・task に live な attempt があることの判定の後に窓を見るので、それらの答えは窓が埋まっていても変わらない。断るときは `StorageError::Capacity`（`leaves_nothing`）で、epoch も tombstone の消去も巻き戻る。
- 送信回数は `placement.submit_sends` に、`submit_attempt` を link へ渡す直前に書く（`count_submit_send`）。答えの無い送信も数に入るので、「送る前の数が 0 で、その送信に `capacity`（または `attempt_id_in_use`）が返った」ときだけ retire なしで閉じる（`policy::sent_once`・`Refusal::Close`）。1 以上、または NULL（分からない）のときは retire を通す（`Refusal::Replace`）。
- retire なしで閉じる処理（`close_unstored`）は、1 つの transaction で、確認済みの tombstone（理由 `capacity_refused` か `attempt_id_in_use`）・attempt の retired・placement の ACK・event を書く。その時点で cancel か retire が記録されていた attempt と in-place の task は閉じずに retire の経路へ回す。retire を送らないので event `retire_requested`・`retire_confirmed` は書かない。
- `attempt_id_in_use` にも同じ規則を当てた（仕様 第11章）。WP24b は送信回数を持たなかったので常に retire していた。
- in-place の task は自動で配置し直さないので、`capacity` も `attempt_id_in_use` と同じく理由なしの `OVERLOADED` として扱い、同じ attempt を後で送り直す。
- 一時的な除外は process の中だけに持つ（`Coordinator::capacity_refused`、worker ID の集合）。`capacity` が返った worker を入れ、その worker の `inspect` が空き（`used < limit`）を答えたら外す（`policy::answers_room`）。入っている間は、親の数え方に関係なくその worker へ配置しない。再起動後は空なので、`inspect` の答えから決め直す。
- それとは別に、断られる前の最適化として、tick ごとの `inspect` の `admission` から「他のメンバーの分（`foreign`）＋親が今その worker に置いている件数（親 DB の記録）」が上限以上なら、その tick はその worker へ配置しない（`policy::window_closed`）。親の分に `inspect` の `own` ではなく親 DB の件数を使うのは、`inspect` の後の同じ tick で retire が確認されて空いた枠を、従来どおりその tick で使うためである。どちらの除外も理由は `capacity`。親自身の件数で満杯（`full`）のときは `full` を先に付ける。`estimate` も同じ判定で外す（集合は読むだけで、変えない）。
- `task_waiting` の `blocked_reason` は、容量で外した worker が 1 台でもあれば `capacity`（全 worker が能力不一致なら従来どおり `no_compatible_worker`）。除外 `capacity` は tick をまたいで続く除外として扱い、変わったときに `task_waiting` を書き直す。
- forecast の `foreign_demand` は、他のメンバーの非 terminal の attempt の件数（node の数ではない）。coverage からは他のメンバーの attempt を除く。配置理由（`Decision` の `compared[].outlook.foreign_demand` と要約の文）と `estimate` の表示に件数を出す。
- A52 の自動試験（WP24c の分。2〜3 メンバーを resident として 1 host に立てる受入は WP24e）: worker の側は `storage::group_tests::a52_the_window_counts_every_member_and_a_refused_submit_stores_nothing`、`worker_agent::group_membership::a52_the_window_is_one_budget_over_every_member_and_a_refused_submit_leaves_nothing`（実 agent に 2 メンバー。`inspect` と forecast が他のメンバーの分を件数だけで示すことも見る）。投げた側は `coordinator::shared_admission::` の `a52_an_attempt_sent_once_and_refused_for_capacity_is_closed_without_a_retire`、`a52_an_attempt_whose_send_is_uncertain_is_retired_before_the_task_moves`（親の再起動をまたぐ場合と、遅れた submit が `RETIRED` になることを含む）、`a52_a_task_with_nowhere_to_go_waits_for_capacity_and_is_placed_when_there_is_room`（local worker が他のメンバーの仕事で埋まっている場合。`estimate` 100 回が何も変えないことも見る）、`a52_the_members_own_tasks_and_the_others_fill_the_window_together`、`a52_an_in_place_task_refused_for_capacity_waits_and_is_neither_moved_nor_failed`、`a52_a_worker_that_refused_for_capacity_gets_nothing_until_it_answers_room`、`an_attempt_id_in_use_is_closed_when_sent_once_and_retired_when_the_send_is_uncertain`。2 つの実 coordinator と 1 つの実 worker の同時配置は `coordinator::shared_admission::a52_two_members_place_on_one_real_worker_at_once_and_its_window_holds`。

鍵の配布・切替・送り直しは SSH 越しの固定 helper で行い、worker protocol の操作にはしない（WP24d）。仕様 §22.3 が定めているのは secret bundle の HMAC であり、hello の認証は §24.2 が新しく定める。鍵そのものは同じ file を使う。

R68 と R96 は backlog の番号をそのまま行の名前にした。R68（resident が自分を更新する）は、どのメンバーも投入する側になることで全員に要るようになったため、初回リリースの範囲に含める（仕様 §24.8）。R96 は、2026-11-20 時点で coordinator の tick が応答しない worker 1 台につき `worker_budget_ms`（既定 15 秒）のあいだ actor を持ち続け、CLI の要求が時間切れになる問題で、仕様 §24.7 を完了条件とする。WP21 の残り（退役した identity の worker へ伝える wire の message。S24 の残り 5）は WP24b が担当する。

WP25cのfast-worktreeとportoolのライブラリ依存は、前提APIを含むマージコミットに固定する。beltをcrates.ioに公開する前に、対応する両ライブラリを公開してからregistryの固定版に切り替え、A48・A55を再検証する。

2026-11-22 に fast-worktree 0.6.0 と portool 0.13.0 を crates.io に公開し、依存を registry の固定版（`=0.6.0`・`=0.13.0`）へ切り替えた。portool 0.13.0 は固定していたコミットに CHANGELOG の記述だけを足したもので、コードは同じである。

## 仕様1.5とS26の非実機受入

検証対象は統合版 `6c3a304` の製品コード（`belt-cli 0.0.0`、release profile）と、`627274d` の A57 追加試験。親は macOS 27.0.1 arm64、Linux は Debian 12 の arm64・amd64。両アーキテクチャで基本の Docker 受入は各9件合格した。組み込みは local-ci 0.18.3、task-traffic-controller 0.10.2、fast-worktree 0.6.0、portool 0.13.0。初回利用は [README](../README.md)、受入環境の起動・更新・撤収は [S26 手順](../scripts/s26/README.md) に従う。

| 経路 | 確認内容 |
| --- | --- |
| 1台での初回利用（A50） | 初期化前のHOMEで `belt exec -- <command>` と `belt --json exec --local -- <command>` が直接実行され、終了コードを引き継ぐ。 |
| 登録と配置（A01・A02・A45） | Dockerで親とworker 2台に仕事が分散し、途中参加したworkerが既存の未配置キューを処理した。 |
| 再起動と時刻のずれ（A24・A26、自動試験） | `tests/it/one_host.rs` で起動 ID が変わった worker の attempt だけが interrupted になり、他方が継続することを検証する。worker 2台の時計を1日遅れと1年進みに設定し、lease 期間を超える両ジョブが1回ずつ完了することも検証する。 |
| ワーカー更新（A34・A35） | 疎通確認の失敗で旧版に戻り、再試行で更新できた。systemdのsystem unitを再起動した後、非root workerが親へ再接続してジョブを実行した。 |
| 実プロジェクト（A37） | 同じコミットの4ジョブについて単独・分散、各2回の結果を照合。単独の初回25.86秒・再実行16.84秒に対し、分散では13.20秒・12.70秒。15テストの結果は一致した。 |
| toolchain（A42・A44） | 実 SSH 接続した Linux worker で Node.js 20.19.0 を明示同期し、20.19.1 をジョブから自動導入した。20.19.2 は `workers diff` が `auto_installable` と導入手順を表示し、同期後に `same` となった。再同期で追加導入は0件、指定版のジョブは成功した。 |
| workspace と組み込みツール（A48・A55） | macOS の実 worker で `.worktreeinclude` の選別、単独版 portool との別割当、hook の有無にかかわらず対象の割当だけが解放されることを確認。Linux worker は外部 CLI のないイメージでジョブ内の組み込み portool を使用。 |
| 一時ワーカー（A46） | SSH転送だけを切断すると既存attemptがunknownを保ち、forget後にinterruptedとなった。旧identityの再接続による再実行は0件。 |
| グループ（A51・A53・A54） | 各投入元だけがそのrunの正本を保持。片方のresidentを停止した間も他の投入元のrunは同じworkerで完走。鍵切替後も初期化済みメンバーとworker専用サービスが利用でき、skip・send-key・removeまで確認した。 |
| メンバー更新（A57） | 更新の試行中は変更要求を拒否。異常時はガードが旧版と両DBを復元し、既存attemptを再実行せずに引き継いだ。75秒のジョブの終了を待つ正常更新も確認。追加の対称メンバー試験では、試用中に2回拒否された retire が復旧後に元の DB で tombstone として確定し、遅延 submit を拒否した。再配置先のコマンド起動は1回。 |

マシンの実際の再起動に伴うboot ID変更、Macのsleep、異なる物理ホスト間のLAN性能は実機検証の対象として残る。Dockerではホストカーネルを共有する。x86_64 Dockerはarm64上のRosettaによる実行。

## 仕様1.6への同期（2026-11-23、main `6875f72` 起点）

この節は、仕様1.6（`docs/spec.md`）を実装に合わせた記録である。上の各節にあった「未実装」「残り」のうち、実装済みになったものには、その行の末尾に日付つきの追記を足した。履歴は消していない。

### 実装済みになったもの

| 項目 | 内容 |
| --- | --- |
| R18 | `estimate --task` が、保存済みの CI task の step ごとの node を使う（配置と同じ `workload_of_spec`）。 |
| R20 | `fail_ci_tasks` が、retire 済みの attempt だけを持つ task も cancel の対象にする。 |
| R62 | `envreport::collect` が policy digest を測る。 |
| R63 | doctor の worker ごとの確認が並行になった。 |
| R64 | `workers sync --project` が workflow の `setup-*` の `*-version` 入力を読む。 |
| R75 | Linux を親にした Docker 受入（S26 の `--linux-parent`、PR #118）。 |
| R78 | 失われた retire の答えの注入を one_host の `Faults` に寄せた（`tests/it/coordinator.rs` の `link_to`）。 |
| D06 | `belt workers` の一覧。 |
| D20 | send-key のときに resident が local link の watch で終了する件。 |

### この数日で入ったもの

- R21（PR #124）: CI task が、提出時に固定した toolchain を持つ。宣言した版を用意できる worker が無ければ配置されず、理由の行は `toolchain node=20.0.0 unavailable` の形。baseline が分からない tool の要求は、提出時に `environment_mismatch` で拒否する。
- R36（PR #119）: fail-fast で止めた実行中の Shard の理由を `ci.unrunnable` に残す。
- R40（PR #120）: `logs <run>` の GitHub status 行。
- R44（PR #122）: `gh` の時間切れで process group ごと止める。
- R70（PR #121）: doctor の `traffic_processes`。
- 直接実行（PR #117 とそれ以前）: `--profile`・`--requires` の拒否、`--json` で command の stdout を stderr へ、`TASK_TRAFFIC_CONTROLLER_NESTED=inherit` で admission を取らない、resident の有無を `startup.lock`・`coordinator.lock` の flock で調べる、belt が端末の foreground process group にいるときは SIGINT を子へ転送しない。
- 鍵の切り替えの後始末（PR #116）: `switching_key` の後始末と、`<key>.installing` への新規作成。
- 埋め込みの道具と片付け（PR #115）: fast-worktree・portool の bin を PATH の先頭に置く。割当の解放は、ディレクトリが消えたときだけ。
- 入力の配送 event `ci_input_delivered`（PR #116）。
- warm-up の解放（PR #129）: resident 上の worker で `supervise` が `reap_warmups` を呼んでいなかった。
- retry の後続 Job（PR #131）: 失敗のために `skipped` になった後続も開き直す。
- `belt ci status`（PR #132）。
- 試験の追加（PR #125・#126・#128）とフレーキーの修正（PR #123・#127）。

### 入れないと決めたもの

- R34: coverage の Job は分割しない。仕様どおりに、merge を持たない runner の Job は分割しない。
- R49・R50: 旧形式との突き合わせ。0.1.0 は初回公開で、旧形式のデータを持たないため。
- R53: HOME が symlink のときは、init を拒否したままにする。
- R54: source と Vault の自動消去は、常に有効にする。
- R74: service user 上の doctor は、実機試験で確かめる。
- R80・R81: 製品に試験専用の feature が要る。A26 と A20 は、既存の試験で満たしている。
- D05、D10: 入れない。

### 試験の書き方の注意

scheduler は、run の入力が届いた remote worker だけを候補にする。特定の配置が要る試験は、タイミングではなく `profile` などの要件で配置を固定する。

### 実機試験（延期中）で確かめるもの

次を、実機試験の一覧に足す。
- 端末で `belt exec` を動かしている間の Ctrl-C が、子に SIGINT をちょうど1回だけ届けること。
- R03〜R08、R10、R12、R14、R66、R74、R93。

version 0.1.0 への変更、`cargo publish`、tag、Release は、実機試験のあとに行う。

## 仕様1.7（第25章: ディスクに残すものと消す時点）の実装

仕様1.7は実装に先行する。実装との差は仕様の25.9にある。実装は仕組みごとに5つに分ける。どの組も、更新の試用中は消す処理を止める（25.1の8）。

1. **runとattemptごとのfileを、刈り取りと終了で消す。** 親: 刈り取りの順序（25.2）のうち、`reader-pins/<run>.lock`、`in-place/<run>.json`、`sources/runs/<run>` とそのlock、`sources/retention.db` のrunの行、bundle、親のGit領域のref。worker: `supervisors/<attempt>.lock` の削除と、全走査をやめること、ACKの直後の削除。触るfile: `src/service/resident/result_retention.rs`、`src/service/resident/input_retention.rs`、`src/results/pin.rs`、`src/results/retention.rs`、`src/workspace/in_place.rs`、`src/source/store.rs`、`src/source/retention.rs`、`src/source/retention/deletion.rs`、`src/service/forward.rs`、`src/supervisor/lock.rs`、`src/worker/agent.rs`、`src/storage/policy.rs`。lockを取る側すべてにinodeの確認を入れる（25.1）。
2. **workerの側のstore（source、clone、toolchain）。** sourceごと・rootごとの最後に使った時刻とbytes、attemptからの参照、14日・20 GiBの削除、cloneのrefの削除と `git gc`、`incoming/` の24時間、受理のときのsourceへの参照、親の `ready` の10分、`install.lock` のinodeの確認。触るfile: `src/worker/machine.rs`、`src/worker/blobs.rs`、`src/worker/blob_policy.rs`、`src/worker/agent.rs`、`src/source/retention.rs`、`src/workspace/mod.rs`、`src/toolchain/adapters.rs`、`src/setup/builder.rs`、`src/setup/policy.rs`、`src/coordinator/input_gate.rs`、`src/service/resident/delivery.rs`、`src/storage/schema.rs`。
3. **crashの受け皿と古いbinary。** 起動時の走査（25.5）、所有marker付きの一時directory（`ci-plan-*`、`belt-log-reader-*`）、`sources/tmp/` と `.s*`・`.tmp*`・`.init-*`、Vaultの一時file、commitでの `bin/<digest>/` と `tool-bin/<digest>/` の削除。触るfile: `src/platform/owned_temp.rs`、`src/service/planning.rs`、`src/service/logs.rs`、`src/service/worker.rs`、`src/service/resident.rs`、`src/secrets/vault/guard.rs`、`src/storage/coordinator.rs`、`src/service/init/upgrade.rs`、`src/service/init/takeover.rs`、`src/service/worker_service/upgrade.rs`、`src/workspace/embedded.rs`。`scripts/s26/down.sh` に `--all` を足す。
4. **journalと行の刈り取り。** 親DB: 「消せない」triggerを外すmigration、`run` の印、`event(run_id, seq)` の索引、行の削除（25.2の6）、runに属さないevent、`ci_test_timing` の時刻、identityの記録。worker DB: attemptの行とeventの削除、重複防止の記録、epochとACKとメンバーの削除による消去。回収を閉じたときは閉じ方に依らず `ack_collected` を送ること（行の無いattemptを含む）。`workers forget <member>` での消去。触るfile: `src/storage/schema.rs`、`src/storage/coordinator.rs`、`src/storage/coordinator/retention.rs`、`src/storage/coordinator/ci.rs`、`src/storage/coordinator/identity.rs`、`src/storage/common.rs`、`src/storage/worker.rs`、`src/coordinator/mod.rs`、`src/results/collect.rs`、`src/service/group/rotate.rs`。
5. **一時workerの起動し直し。** 所有marker `ephemeral.json`、前のprocessが居ないことの証明、残ったattemptの停止、stateの削除（25.3）。触るfile: `src/service/worker/ephemeral.rs`、`src/service/worker/ephemeral/plan.rs`、`src/platform/owned_temp.rs`、`src/supervisor/recover.rs`。

順序: 4の `run` の印と行の削除は、1の親の側より先か同時に入れる（印が無いと、刈り取りの途中で落ちたrunを続けられない）。1のACK直後の削除は、4のworkerの重複防止の記録と同時に入れる（行を消すと、遅れたsubmitを断る記録が無くなる）。2は独立。3の起動時の走査は、1と4の後に入れると対象が決まる。5は独立。組み込んだライブラリ（task-traffic-controller、fast-worktree、portool）の自動保持は、各ライブラリの版の採用時に、beltが呼ぶ関数を仕様の25.4の表に書き足す。未決（仕様の25.9）: 常駐workerの退役させたidentityの記録を消す時点、親のGit領域の `refs/local-ci/heads/*`。
