# belt 仕様

仕様版 1.7 / 2026-11-23（1.6: 2026-11-23、1.5: 2026-11-21、1.4: 2026-11-20、1.3: 2026-11-10、1.2: 2026-11-10、1.1: 2026-11-09、1.0: 2026-11-06）/ 実装に向けた提案仕様

この文書は、`belt` を日常の開発で使える状態まで一度に実装するための仕様である。未コミット変更、複数Macへの配置、導入・起動、ログ・成果物、キャンセル、切断・再起動からの復旧を、初回リリースの必須条件とする。実装工程は分けるが、これらが揃う前を実用版の完成とは扱わない。

会話で確定した要求と、この文書で具体化する設計を区別する。以下のコマンド、API、設定名、既定値は新設する仕様案であり、現行リポジトリに存在する機能を示すものではない。

1.1 での変更: 第22章（環境の整合）と第23章（設計判断と基準実装の更新）を追加し、受入条件 A42〜A44 を足した。

1.2 での変更: workerのattempt workspaceをfast-worktreeで作る・消す方針を22.5として加え、依存の準備の担当を第6章・第8章で整理した（`.fast-worktree.toml` を優先し、`[prepare]` は代替）。fast-worktreeを22.4の周辺toolに加えた。artifactの範囲を「同一run内の同じworkflow内」に改めた（第7章・第13章）。lib化後の基準版を task-traffic-controller 0.5.0、local-ci 0.9.0 に更新した（第16・17・21・23章）。基準プラットフォームにLinux（x86_64・arm64）を加え、Google Colab等の常駐しない一時workerと `workers forget` を定めた（第1・4・12・15章）。`.worktreeinclude` に一致するignoredファイルをsecret_filesと同じ経路で自動送付する（第6章・22.3）。受入条件 A45〜A49 を足した。worker の priority（配置の好み）を設定に加え、配置の比較値に補正量として入れた（第5章・A49）。

1.3 での変更: 仕事の呼び名を旧名から `task` に改めた（ID prefix は `task`、旧名の別名は置かない）。local-ciの Task は混同を避けるため `ExecutionTask` と書き分けた。

1.4 での変更: 親をclusterに1つ置く構成をやめ、マシンを対等なメンバーからなる1つのグループとして扱う構成に改めた。どのメンバーも投入でき、runの正本は投げたメンバーだけが持つ。第24章を追加し、第1〜5章、第9〜12章、第15・16章、第18〜20章、第22章の該当箇所を合わせた。グループ鍵による認証とメンバーの追加・削除・鍵の更新（`group`）、投げたメンバーをまたぐ調停と容量による断り、退役の通知、遅いpeerが他を止めない要求（R96）、メンバーの更新（R68）を定め、受入条件 A51〜A54 と A56〜A58 を足した（A50 は `belt exec`、A55 は fast-worktree・portool の組み込みのために空けてある）。

1.5 での変更: `belt exec` を1つにまとめ、実行先は配置が決めること、`--local` で手元に固定できること、常にadmissionを通ることを定めた。residentが動いていないときは手元で、単独の `task-traffic-controller` CLIと同じ台帳・scope・policyで動く（第2〜4章・第6章・第16章・第18章）。fast-worktreeとportoolを別配布のtoolからbeltに組み込むライブラリに改め、プロジェクトのhookやcommandが呼ぶ `portool`・`fast-worktree` の扱いと、単独CLIとの共存を定めた（第3章・22.2・22.4・22.5）。受入条件 A50 と A55 を足し、A44 と A48 の文言を改めた。5.1のbareの `portool`・`fast-worktree` を組み込みの版で満たすものとした。第7章に、分割したJobのcoverageとJUnitの集約の範囲を定めた。

1.6 での変更: 実装に合わせて記述を改めた。直接実行（residentが動いていないときの `belt exec`）の拒否するoption、`--json`、nested、residentの有無の判定、SIGINTの扱い（第16章）、CI taskの入力の配送のevent `ci_input_delivered` と、retryでの後続Jobの開き直し、fail-fastで止めた実行中Shardの理由（第7章）、`logs` のGitHub status行（第23章）、CI taskのtoolchainと、版を測れないtoolの提出時の拒否（第5章・22.1）、`doctor` の `traffic_processes`（第16章）、setupの見積が不明のときの扱い（第9章）、`belt ci status` による投稿のやり直しと `gh` の時間切れ（第17章）、鍵の切り替えの後始末とworkerが受け取る鍵の置き方（24.2）、Vaultの分類pinと途中まで公開されたmarker（第23章）、組み込みのfast-worktree・portoolのPATHと割当の解放（22.5）、`workers` の一覧の形式と `workers forget` の範囲（第4章・第15章）。

1.7 での変更: 第25章（ディスクに残すものと消す時点）を追加した。beltがディスクに残すものすべてについて、読み手、今の挙動、消す規則、使われていない証明を挙げ、消す処理を段階0〜5に分けて定めた。第25章は実装に先行し、段階が実装されるまでは他の章の記述が実装の挙動である。段階が変える他の章の文は25.14に挙げ、該当の文に注記を付けた。

# 1. 製品の目的と確定事項

`belt` は、手元のMacとLAN内の複数Macを使ってCIと開発用コマンドを実行する、単一コマンドの実行基盤とする。利用者は仕事を投入し、配置・待機・実行・回収をツールに任せる。

| 項目 | 契約 |
| - | - |
| 公開コマンド | `belt` に統合する |
| 仕事の呼び名 | `task`（beltが配置する仕事単位。local-ciのJob DAGの `ExecutionTask`（配置可能な実行セッション）とは別概念で、`ci` はCI runを作りその中のJob／ExecutionTask／Shardを `task` にする） |
| 構成 | マシンは対等なメンバーからなる1つのグループを作る。どのメンバーも同じ `belt` で、投入もでき、他のメンバーが投げた仕事も受ける。以下の「親」は、そのrunを投げたメンバー（のcoordinator）を指す（第24章） |
| runの正本 | runごとに1つ。投げたメンバーのSQLiteに置く。合意の仕組みや分散DBは使わない |
| 親の計算資源 | 親自身もworkerとして参加する |
| 実行場所 | 通常、発注側は指定しない。能力・待ち時間・準備時間から親が選ぶ |
| 対象 | `local-ci` によるCIと、一般の非対話コマンド |
| 最初の実用版 | 未コミット入力、動的配置、導入・起動、回収、停止、復旧を含む |

基準プラットフォームはmacOS 14以降のarm64とx86_64、およびLinuxのx86_64とarm64とする（Linuxの追加は2026-11-10の決定）。親のcoordinatorもworkerも両OSで動く。Google Colabのような常駐しない環境は、Linux上の「一時worker」として扱う（第15章）。これは新しい製品の対応方針であり、現時点の動作確認済み範囲ではない。リリース時に対応OS・CPUで検証する。

分散するのは独立して実行できる仕事である。1つの通常プロセスが使うメモリを複数Macのメモリにまたがって確保する仕組みではない。1つの巨大な処理は、実行に必要な資源を持つMacへ配置するか、明示した分割契約によって複数のtaskにする。

# 2. 初回リリースの完成範囲

次をすべて必須とする。

1. `belt ci` と `belt exec -- <command>` が、親と子を同じ候補として扱う。
2. 変更中のソースを固定し、未コミット・未追跡・未pushの内容をリモートでも正しく実行できる。
3. CPU・メモリ・ローカルキュー・実行履歴・転送・setupを考慮して配置する。
4. 未配置の仕事を親に保持し、空いたworkerや途中参加したworkerへ動的に割り当てる。
5. 親からSSHでworkerを登録、導入、起動、停止、更新できる。
6. CLIを閉じても受理済みの仕事とログが失われず、再接続で追跡できる。
7. 切断・ACK消失・親やworker agentの再起動で二重実行を起こさない。
8. 未開始・実行中の仕事を取り消せる。停止確認前に停止済みと表示しない。
9. 必須成果物と終了結果を検証して回収でき、回収だけを再開できる。
10. 既存の `local-ci` と `task-traffic-controller` のCLI互換を維持し、同一ホストの資源予算を二重に作らない。共有scopeでの保証には、後述するtask-traffic-controller runtime更新を必要とする。
11. `needs` を保持したJob DAGと独立Shardを複数workerへ配置できる。長時間テストの自動shardingも初回実用版の対象とする。
12. 実際の2台以上のMacと障害注入で、後述の受入条件を満たす。
13. worker間のツール版・入力ファイルの差を検出し、自動で揃えられるものは揃え、揃えられないものは理由と手順を示す（第22章）。
14. Linux（x86_64・arm64）のworkerを、macOSと同じ契約で常駐・導入・更新できる。常駐しない一時worker（Google Colab等）を、親からSSH経由で使い、消滅を利用者が宣言できる（第15章）。
15. workerのattempt workspaceを、プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` をそのまま使って、beltに組み込んだfast-worktreeで作り、消す。workerに入れるのはbeltだけで、fast-worktreeとportoolの別インストールを要求しない（22.4・22.5）。
16. `init` 済みのどのメンバーからでも投入でき、投げたメンバーがそのrunの正本を持つ。1つのworkerがグループの全員の仕事を受け、取り合いを受ける側で調停する。投げたメンバーが1つ落ちても、他のメンバーのrunは残りのworkerの上で続く（第24章）。
17. メンバーであることをグループ鍵で確かめ、メンバーの追加・削除と鍵の更新を2〜5台で無理なく行える（24.2）。
18. 遅い・応答しないメンバーが1台あっても、CLIの応答と他のメンバーとのやり取りを止めない（24.7）。
19. メンバーが自分のresidentを更新でき、失敗したら二重実行を起こさずに元の版へ戻せる（24.8）。
20. residentが動いていないマシンでも、`belt exec -- <command>` が手元で動き、単独の `task-traffic-controller` CLIと同じ予算を共有する（第4章・第16章）。

対象外は、投げたメンバーが落ちたrunを別のメンバーが引き継ぐ自動フェイルオーバー、メンバーの自動発見、任意プロセスの途中状態の移送、電源OFFのMacの確実な遠隔起動、任意のGitHub Actions actionへの完全互換、共有メモリ型の分散処理とする。これらは今回の開発目的を定義する境界であり、上記の実用機能を後回しにする理由にはしない。

# 3. 責務と内部構成

| 層 | 担当 | 担当しないこと |
| - | - | - |
| belt CLI | 投入、観察、キャンセル、worker管理 | workerの選択を利用者へ要求しない |
| belt coordinator | durableなキュー、配置、attempt管理、集約、GitHub status | リモートプロセスの資源を親の測定値で代用しない |
| belt worker agent | 受理、入力準備、ローカル実行管理、spool、再接続 | 分散先を独自に選び直さない |
| attempt supervisor | 1つのattemptの寿命、lease、step・サービスの管理 | SSHセッションの寿命に従属しない |
| command supervisor | 1つの重いcommandのadmission、起動、計測、停止 | worker全体のsignal handlerを共有しない |
| local-ci-core | workflow解析、Job DAG、実行可能ExecutionTask／Shardへの分割、CI実行・結果 | ネットワーク配置を所有しない |
| task-traffic-controller-core | 1ホスト内の資源計画、キュー、開始判定、見積もり、計測 | ネットワークや分散リーダーを管理しない |
| fast-worktree・portool（ライブラリ） | attempt workspaceの作成・削除・pool・hook、ポートなどのローカル資源の割当・解放（22.5） | 配置やadmissionを持たない |

coordinatorとworker agentは、1つのメンバーの1つのresidentの中で両方動く（第24章）。coordinatorは自分が投げたrunだけを所有し、worker agentはグループのすべてのメンバー（自分のcoordinatorを含む）から仕事を受ける。表の「親」は、そのrunを投げたメンバーのcoordinatorを指す。

`task-traffic-controller` に分散システム全体を入れない。ホスト内の資源制御を再利用可能にし、その情報を `belt` が配置判断に使う。

配布物は原則として1つの `belt` バイナリとする。通常利用で `local-ci`、`task-traffic-controller`、`fast-worktree`、`portool` の別インストールを要求しない。既存リポジトリから `local-ci-core` と `task-traffic-controller-core` を抽出し、既存CLIもそのcoreを使う薄いadapterとして維持する。fast-worktree（0.7.0）とportool（0.14.0）も、crates.ioに公開したライブラリとして組み込む。どちらの単独CLIも公開を続け、手元の開発環境（編集を続けるworktree、devサーバー、Composeのポート分離）にはそれを使う。beltには `worktree`・`port` のような入口を作らない。依存version・protocol・cache schemaは固定し、配布物に元のライセンス表記を含める。

workerには待受プロセスを置く。ただしLANへ独自の未認証TCPポートを開ける構成を既定にはしない。親がSSH接続を張り、`belt worker --stdio` をUnix socketへのbridgeとして起動する。agentとsupervisorはSSHから独立して動く。親のlocal workerも同じ実行経路を使い、通信部分だけUnix socket直結とする。他のメンバーがSSH越しに使うworkerと、そのマシンのlocal workerは同じ1つのworker agentである（24.1）。

attempt supervisorは、attemptごとに同じ `belt` バイナリを別プロセスとして起動したものとする（第23章）。

# 4. 利用者が使うコマンド

## 初回セットアップ

```sh
belt init
belt workers add mini-a --ssh ci@mini-a.local --install
belt workers add mini-b --ssh ci@mini-b.local --install
belt doctor
```

`init` はそのマシンを、coordinatorと自分のlocal workerを持つメンバーとして構成する（macOSはLaunchAgent、Linuxはsystemdのuser service）。グループにまだ属していなければ、そのマシンだけのグループ（IDと鍵）を作る。1台だけならこれで使える。`workers add --install` はSSH接続、互換バイナリ転送、service登録、起動、能力検査を実施する。親が送るのは、workerのOSとarchが親と同じときは親自身のバイナリ、違うときは `--binary <path>` で指定した、そのworker向けのbuildである（指定が無ければ、必要なtarget tripleを示して断る。第15章）。`workers add` は相手を自分のグループのメンバーにし、グループのIDと鍵を渡す（24.2）。相手が別のグループに属するときは断り、`--move` を明示したときだけ移す。相手が既にbeltを持つマシンなら、2つ目のworkerを導入しない（`--install` を付けても同じ。版が合わないときは `workers upgrade` を案内して止まる。第15章）。再実行は冪等とし、途中失敗を再開できる。SSHのRemote Loginと認証が利用可能であることは前提とする。OSの権限やライセンス同意が必要な環境導入は、必要な操作と検出した不足を明示する。

## 日常の利用

```sh
belt ci
belt ci plan
belt ci --snapshot commit --ref HEAD

belt exec -- pnpm test
belt exec --local -- cargo build
belt submit -- cargo test

belt ls
belt show <run-or-task-id>
belt logs <id> --follow
belt wait <id>
belt cancel <id>
belt retry <id>
belt outputs <id> --dir ./belt-results
```

どのメンバーからでも投入できる。ふだん使わない方のPCで `belt ci` を実行すれば、そのPCがそのrunを投げたメンバーになり、自分と、設定に書いた他のメンバー（ふだんのmain PCを含む）へjobを配る（24.1）。これらのcommandが扱うのは、自分が投げたrunだけである。

`ci` はCI runを作り、その中のJob／ExecutionTask／Shardをtaskにする。`exec` は1つのtaskを作り、結果まで待つ。`submit` は非同期投入で、親が入力snapshotと実行仕様を永続受理した後にIDを返す。workerでの受理や実行開始までを保証する応答ではない。

`exec` は1つだけで、手元専用の別コマンドは持たない。実行先は配置（第9章）が決め、利用者は選ばない。グループに他のメンバーもworkerも無ければ、自分のlocal workerで動く。`--local` を付けると、他に候補があっても自分のlocal workerに固定する。`--in-place` は `--local` を含む（第6章）。どのworkerで動いても、そのworkerの受付窓（`max_active_task + prefetch_per_worker`。第9章・24.4）とtask-traffic-controllerのadmissionを通る。admissionを省く指定は設けない。

residentが動いていない（coordinatorもworker agentも無い）マシンでは、`exec` はresidentを起動せずに、呼び出したprocessの中で手元に実行する（直接実行）。commandは呼び出し時のcwdと環境でそのまま動き、snapshot・run・attemptを作らない。admissionは組み込みのtask-traffic-controller coreが行い、単独の `task-traffic-controller exec` と同じ台帳・scope・policyを使う（第16章）。設定に他のメンバーやworkerがあっても配らず、residentが動いていないので手元で実行した旨をstderrに1行出す。`--local` は受け付けるが、直接実行は元から手元なので働きは変わらない。直接実行では、runを前提とする `--snapshot` と `--in-place` を使えず、residentの起動の手順（第15章）を案内して断る。`submit` も同じく断る。`--json` の結果は `mode: "direct"` を持ち、run IDを持たない。

通常のCLI終了・ターミナル切断はキャンセルを意味しない。foregroundの `exec`／`ci` に対するCtrl-Cは明示的なキャンセル要求として扱い、停止確認を待つ（`exec` の直接実行では第16章のとおりcommandを止める）。接続できなければ `cancel_requested / unknown` と表示してIDを残す。`retry` は停止または終了を確認したterminal attemptに対して、新しいattemptを作る。同じrunのretryは元のsource snapshotを使う。現在のファイルで試すときは新しいrunを投入する。

機械向けには各操作へ `--json` を用意し、`schema_version`、ID、状態、error codeを返す。進捗はstderr、構造化結果はstdoutとする。`exec`／`wait` は正常終了なら0、コマンド失敗なら可能な範囲で元のexit codeを返す。signal、入力不備、通信未確定、成果物回収失敗はJSON上で区別する。CLIプロセスの終了コードだけをjobの確定状態と見なさない。

## workerの管理

```sh
belt workers
belt workers start mini-a
belt workers drain mini-a
belt workers resume mini-a
belt workers stop mini-a
belt workers upgrade mini-a
belt workers diff mini-a
belt workers sync mini-a
belt workers forget colab-1
belt estimate --json -- pnpm test
```

`belt workers` は登録したworkerを1行ずつ出す。形式は `id  ssh  ephemeral|persistent  connection  identity|-` とし、列は空白2つで区切る。`connection` は `connected`・`disconnected`・`unknown` のどれかで、residentが実際に観測できたときだけ前の2つになり、residentが動いていないとき、または観測できなかったときは `unknown` とする。`identity` は観測した現在のidentityで、無ければ `-` とする。登録が1つも無いときは `no registered workers` と出す。`--json` は `workers` の配列に `id`・`ssh`・`ephemeral`・`connection`・`identity` を入れる。residentが動いていなくても登録の一覧は出せる。

相手のマシンは他のメンバーの仕事も受け、自分でも投入する（第24章）。`drain`・`resume` はそのworker全体に働く。`stop` はそのマシンのresident（両方の役割）を止める操作なので、相手が他のメンバーの仕事か、自分で投げた非terminalのrunを持つときは、理由を示して断る。`stop --all-submitters` を明示したときだけ、drainの後に止める（そのマシンが投げたrunは、process停止と同じ扱いになる。24.6）。`stop --cancel-running` も同じ条件で `--all-submitters` を要する。`start` はresidentを起動し、両方の役割が戻る。`forget` は、一時workerのほか、鍵の更新で外したメンバーにも使える（24.2）。

`drain` は新規配置を止め、受理済みの仕事は完了させる。通常の `stop` はdrainして終了を待ってから停止する。即時停止は `stop --cancel-running` で明示し、停止確認まで追跡する。到達不能なら停止済みにはしない。`upgrade` は `workers upgrade <worker> --binary <path>` で、親とOS・archの違うworker向けのbuildを指定できる。`diff`／`sync` は第22章で定める。`forget` は、到達できなくなった一時workerの消滅を利用者が宣言する操作で、第15章で定める。一時workerは `workers add <worker> --ssh <dest> --ephemeral` で登録する。

## グループの管理

```sh
belt group
belt group rotate-key [--remove <member>] [--skip <member>]
belt group send-key <member>
belt upgrade
```

- `group` は、グループのID、鍵の世代、自分の名前と、メンバーごとの状態（接続、鍵が合っているか、自分が置いている仕事と受けている仕事の件数）を示す。
- メンバーの追加は `workers add` で行う。削除は `group rotate-key --remove <member>` で、一覧から消して鍵を更新する。メンバーごとの信頼の登録・取消の操作は無い。鍵の更新はSSHのアクセスを取り消さないので、外したマシンのSSH鍵を消す手順を表示する（24.2）。
- `group rotate-key` は、既に次の鍵を持っていれば、同じ指紋の鍵で途中から更新を再開する。相手が別の指紋の次の鍵を持つ場合は断る（24.2）。
- `group send-key` は、鍵の更新に取り残されたメンバーへ今の鍵を送り直す。
- `upgrade` は自分のマシンを更新する（24.8）。

表示は最低限、worker、能力、接続状態、running／waiting、資源予算、予測の品質を含む。taskには担当worker、実行段階、ローカル待ち時間、見込み完了時間、source ID、attempt IDを表示する。未知の時間を0秒と表示しない。

# 5. 設定と能力の照合

マシン・SSH・serviceの情報はユーザー設定へ置き、プロジェクトには必要能力と実行契約を置く。

ユーザー設定例：`~/.config/belt/config.toml`

```toml
schema_version = 1

[coordinator]
local_worker = true
local_worker_priority = -1   # 手元のMacは後回し（既定0、-100〜100、大きいほど優先）
heartbeat_seconds = 5
lease_seconds = 60

[scheduler]
strategy = "earliest_finish"
forecast_ttl_ms = 2000
prefetch_per_worker = 1
priority_step_seconds = 60   # priority 1段 = 60秒分の補正（既定60、0〜3600）

[defaults]               # プロジェクトが上書きできる既定値（最も低い設定層）
timeout_seconds = 3600
toolchain_match = "exact"

# メンバーの一覧。このマシンが仕事を配る相手を書く。自動発見はしない（第24章）
[[workers]]
id = "mini-a"
ssh = "ci@mini-a.local"
service_mode = "user"
binary = "/srv/ci/.config/belt/bin/current/belt"  # `workers add --install` が書く、worker上の絶対パス
priority = 10            # 最優先のworker
labels = ["macos", "arm64", "node"]

[[workers]]
id = "mini-b"
ssh = "ci@mini-b.local"
service_mode = "system-user"
labels = ["macos", "x86_64", "node", "docker"]

[[workers]]
id = "colab-1"
ssh = "colab-1"          # 利用者が用意したトンネルを指すssh設定の別名
ephemeral = true         # 常駐しない一時worker（第15章）
priority = 0
labels = ["linux", "x86_64", "node"]
```

プロジェクト設定例：`.belt.toml`

```toml
schema_version = 1

[source]
mode = "worktree"
include = []
exclude = ["belt-results/**"]
secret_files = [".env.test", "packages/*/.env.test"]

[execution]
profile = "web-dev"
requires = ["macos", "node"]
pass_env = ["CI"]
timeout_seconds = 3600

[toolchain]
match = "exact"

[ci]
workflows_dirs = [".github/workflows"]
no_cache = ["ci.yml", "e2e.yml:browser"]   # 結果を再利用しないworkflow、またはjob
status_context = "local-ci"
unsupported = "error"

[[outputs]]
name = "test-results"
paths = ["test-results/**", "coverage/**"]
required = false
when = "always"
```

`[[workers]]` がメンバーの一覧である。beltはここに書かれたマシンだけへ仕事を配り、LAN内のマシンを自動で探さない。`id` は相手のメンバーの名前と一致させる。一覧はメンバーごとのもので、他のメンバーの一覧と一致している必要は無い。グループのIDと鍵は設定fileには書かず、`init`・`workers add` が置くstateに持つ。`[coordinator] max_active_task` と `[scheduler] prefetch_per_worker` は、自分が配るときの上限であると同時に、このマシンのworkerが他のメンバーへ答える受付窓でもある（24.4）。

設定の優先順位は、明示CLI引数、プロジェクト設定、ユーザー設定、既定値とする。旧 `.local-ci.toml` は対応する項目のfallbackとして読む。両方で矛盾する設定は出所を表示し、黙って片方を無視しない。未知の設定名はerrorにする（`secret_files = [{ glob = ".env", requird = true }]` のように表の内側の綴り誤りも、キー名と位置を示してerrorにする）。

ユーザー設定の `[defaults]`（`timeout_seconds`、`toolchain_match`）は、プロジェクトが書かなかったときの値で、優先順位は「明示CLI引数、プロジェクト設定 `[execution] timeout_seconds` と `[toolchain] match`、`[defaults]`、既定値」とする。ユーザー設定が決めてよいのはこの2項目だけで、`requires`・`secret_files`・`outputs` 等のプロジェクト固有の契約は置けない。

`.local-ci.toml`（local-ci 0.9）の項目は次のとおり扱う。list項目は、順序と重複を無視した集合として比べて矛盾を判定する。

| `.local-ci.toml` | 扱い |
|---|---|
| `workflows-dirs` | `[ci] workflows_dirs` のfallbackとして読む |
| `[source] include` / `exclude` / `secret_files` | 同名の `[source]` 項目のfallbackとして読む |
| `no-cache` | `[ci] no_cache` のfallbackとして読む。対象は `ci.yml`（workflow）または `ci.yml:e2e`（job）。実際の効果は第8章 |
| `link-node-modules` | 対応しない。値があるときだけ `ci plan` と `doctor` がnoticeとして表示する（依存の準備はfast-worktreeの `[[clone]]` が担う） |
| `[setup] ignore-env` | 同上 |

liveness設定は、heartbeat < lease、stale ≥ heartbeat × 2、lease > stale（stale < lease）を検査し、破れる組み合わせはerrorにする。値は0を許さない。一時worker（`ephemeral = true`）はserviceを持たないため、`service_mode` を書くとerrorにする。

workerが申告する能力には、OS・arch・実行user・有効なprofile、tool version、Docker実行可否、利用可能disk、資源予算、protocol、feature flagsを含める。profileは実行PATH、shell、必要tool、許可するenv・secret参照、cache条件を定義し、内容のdigestで識別する。ラベルだけで実際のtool検査を代用しない。

配置前に能力を絞る。候補がないときは `blocked: no_compatible_worker` とし、不足能力を表示する。ただし、提出時に版を測れないtoolの要求は、配置の前に `environment_mismatch` で拒否する（22.1）。同じリポジトリであってもarm64用の依存cacheをx86_64で使わない。Rosetta等の別実行環境は別profileとする。自動導入で解消できる不足は、第22章の手順で導入してから候補に含める。

## 5.1 worker-local profile と generic requirements の実行契約

worker は private な `<worker_state>/profiles.toml`（schema_version=1、`active`、`[profiles.<name>]`）から resident ごとに 1 つの active profile を選ぶ。ファイルは symlink を追わず同じ FD から読み、所有 UID・private permission・通常ファイル・64KiB 上限を検証する。active が定義されていない場合は起動を拒否する。ファイルが無い場合は既に保存された worker env の PATH・SHELL（省略時 `/bin/bash`）から default を合成して通常どおり hash する。project の `[execution] profile` と CLI の `--profile` は worker の active 名を照合する selector であり、worker identity を指定したり環境へコピーしたりしない。profile 変更は drain/restart を伴う operator 操作とする。

profile は絶対 `path` 配列、絶対 `shell`（bash/sh/zsh）、`env` の非 secret 定数、`allowed_env`、`required_tools`、`cache_namespace` を定義する。`secret_refs` が空でなければ安全な provider が未接続である旨を起動時に返す。任意の source/script probe と install adapter はこの schema では受け付けない。自動導入抑止の env は [Corepack](https://github.com/nodejs/corepack/blob/main/README.md)、[rustup](https://rust-lang.github.io/rustup/environment-variables.html)、[mise](https://mise.jdx.dev/configuration/settings.html) の設定に従う。明示 profile では command/setup env は operator の allowlist に従う。合成 default は既存の通常 command env を許す。いずれも PATH・SHELL・HOME・TMPDIR の異なる上書きを拒否し、同じ値の指定は許す。暗黙の版導入は許可せず、Corepack の network/auto-pin/latest/download-prompt、rustup の auto-install、mise の auto-install を無効化する env を固定し、task から異なる値で上書きできない。probe、Machine、setup、consumer は同じ active PATH/env を使用し、shell を省略した setup は profile の絶対 shell を login/rc 読み込みなしで実行する。

`requires`（CLI は繰返し/カンマ区切り `--requires`）は AND 条件とし、重複を除く。bare の `macos/linux/windows`、`arm64/aarch64/x86_64/amd64`、`node/npm/pnpm/yarn/rustc/cargo/flutter/dart/xcode/git/portool/fast-worktree/docker`、同じ登録済み値の `os:/arch:/tool:`、登録済み protocol feature の `feature:worker_capabilities_v1/blob_resume/log_resume` を受け付ける。arch は aarch64/x86_64 に正規化する。unknown bare/prefix/tool/feature は invalid input とする。bare の `portool` と `fast-worktree` はbeltに組み込んだ版（22.4）で常に満たされ、PATHの実体を検査しない。ラベルは tool 検査の代わりにならない。この generic requires は version constraint ではなく、版の宣言・照合・導入は §22.1 に従う。

tool は profile PATH で実行可能な実体を解決し、固定 argv・env_clear・private probe cwd・各 300ms・stdout/stderr 各 8KiB の上限で版を検査する。所有する子 process group だけを終了し reap する。resident の起動時（profile 読み込み時の required tool の検査と、baseline を公開する最初の report）だけは、各 tool の結果が確定（available / unavailable / 失敗による unknown）するまで測る。1 回の呼び出しは 2 秒まで待ち、時間切れになった呼び出しだけを tool ごとに最大 3 回、全 tool で共有する 5 秒の範囲で繰り返す。共有時間を使い切った後も各呼び出しは 300ms は待つので、待ちの上限は読み込み・起動 report それぞれ 5 秒 + 最大 14 呼び出し × 300ms = 9.2 秒である。失敗や解析不能は確定した結果であり繰り返さない。それでも時間切れの tool は unknown のまま公開し、baseline の公開後に観測できた場合は profile の drift として扱う（再起動まで worker を block する）。Docker は `info`、Xcode は選択済み SDK path の確認も必要で、binary の存在だけでは available としない。public report へは検証済み版と identity digest を出し、環境の値や private path は出さない。未観測の disk/budget は unknown とし、available/0 を捏造しない。Flutter launcher と Dart の bootstrap wrapper は版の問い合わせでも SDK 導入が発生し得るため、S20 の noninstalling adapter が接続されるまでは Unknown として扱う。native Dart binary は通常の固定 probe を使う。

`worker_capabilities_v1` を hello で交渉し、hello/inspect に optional report を含める。制約がある task は有効な report がない旧 peer へ配置しない。制約は TaskSpec と AttemptPayload に保存し、plan/payload digest に含め、retry は元の条件を保つ。空の条件は JSON から省き、旧 payload の canonical JSON/hash を保つ。durable requirements を知らない旧 binary の再開を防ぐため、coordinator/worker DB の schema version に semantic compatibility fence を設ける。readonly 旧 schema は migrate せず、配置診断用 index が無ければ placement を返さない。性能だけのための index（placement の未 ACK、attempt の生存）は feature marker を持たない。version の増加で旧 binary が拒否され、readonly の旧 schema は index が無いだけで同じ結果を返す。constrained submit は private resident IPC v3 を要求し、requirements を知らない旧 owner に条件を落とされないよう受理前に拒否させる。制約なし IPC は v2 のまま保つ。Coordinator は forecast 前に同じ pure predicate で候補を絞り、除外された worker ごとの不足と `no_compatible_worker` を配置記録へ残す。公開 Task metadata に最新の placement を含め、最大 4 worker × 各 4 不足（各 128 文字）へ明示 truncate する。受理済み/取消済み task へ古い待機理由は表示しない。worker も新規受理時と workspace 準備直前、長い setup 後に再検査する。受理済みの同じ attempt/hash の replay は元の応答を保つ。

実測 profile fingerprint は worker-local identity であり、別 host に同じ絶対 path/digest を要求しない。profile 名と実 requirements で照合する。受理時 fingerprint は worker の private durable metadata、setup/template、artifact、TTC history に固定する。tool/report drift は新規 report を無効にし、古い template や異なる実環境で実行を続けない。 startup report で Available だった tool の一時的な Unknown は、shell・PATH 上の実行ファイル identity が不変なら startup fingerprint を保持する。ただし現在の report は Unknown のままで、requires・profile の required_tools・固定 native baseline を満たさない。既知の版/identity の変化、実行ファイルの変化、startup Unknown から Available への変化は新 identity として扱い、同じ owner の既存 fingerprint では受理しない。再起動後も旧 attempt の実 identity を書き換えない。版管理 shim が project の版ファイルを読む場合の具体版 activation と baseline は §22.1 の必須残件であり、この generic probe だけで version matching の完了とはしない。


# 6. 入力snapshotと作業ディレクトリ

## 既定は現在の作業内容

`belt ci` と `belt exec` は、既定で呼び出し時のworktreeを固定する（residentが動いていないときの `exec` の直接実行は、固定せずにその場で動く。第4章）。commit済みの内容だけを実行するときは `--snapshot commit --ref <ref>` を明示する。既存の単独 `local-ci run` の既定動作は変更しない。

worktree snapshotには、追跡済みファイルのstaged／unstaged変更、削除、実行bit、binary、symlink、gitignore対象外の未追跡ファイルを含める。tracked fileはignoreに一致しても収集し、ignoredな未追跡ファイルは明示したincludeだけを追加する。`secret_files` と `.worktreeinclude` に一致するignoredファイルはsnapshotに含めず、第22章の別経路で扱う。コピー対象の拡張は `ci plan`／dry-runで確認できる。原本のworktree、index、branch、refsを書き換えない。

収集と検証の手順は次とする。

1. HEAD、index tree、論理branch、base・merge-base、収集対象一覧を記録する。
2. 内容を、その取り込みだけのscratch directory（private object storeと同じfilesystem）へ取り込み、path・mode・size・hashのmanifestを作る。この時点では、object storeには何も入れない。
3. 内容とGit metadataを再検証し、収集中の変更を検出したら最大3回まで再試行する。
4. 安定した内容が得られた場合だけ、objectとmanifestを、置き換えをしないlinkでprivateなobject storeへ入れて `source_id` を確定する（objectが先、manifestが最後。入れる前に、入れるfileを保持の索引に書く。25.4の (b)）。
5. 確定できなければ `SOURCE_BUSY` とし、workerへdispatchしない。

これは検証付きのcopyによる固定であり、編集が並行するファイルシステム全体の厳密な同時点snapshotを保証するものではない。1つのrunでcaptureするのは1回とし、途中参加worker、再配置、再接続、retryも同じ `source_id` を使う。

## Gitの実行文脈

`SourceManifest` は少なくとも次を保持する。

| フィールド | 意味 |
| - | - |
| source_id / manifest_digest | 入力内容を固定するidentity |
| repository_id | ホストの絶対pathに依存しないrepo identity |
| source_kind | worktree、commit、directory |
| original_head / index_tree / worktree_tree | 元のGit状態と固定した内容 |
| execution_commit | CI用に作るsynthetic commit、または指定commit |
| base / merge_base / branch | 差分・条件式のための論理文脈 |
| cwd_relative | repo rootからの相対作業path |
| object_manifest | 未pushのcommitを含む必要Git objects |
| submodules / lfs | 固定したrevisionと必要payload |
| secret_files | 第22章のsecret bundleのHMAC digestとpath一覧（`.worktreeinclude` 由来を含む。内容は含めない） |

generic execでは可能な範囲でHEAD・index・worktreeの区別を再現し、`git diff` と `git diff --cached` が元の変更を表せるようにする。CIでは同じcaptureからsynthetic commit viewを作り、その内容でworkflow・config・changed filesを解釈する。元HEADやbranchの論理文脈は別フィールドで渡す。

必要なGit objectsとbase履歴をbundle等で転送する。workerが移動中のorigin branchをfetchして入力を決め直す方式は使わない。workerのcanonical cloneはoriginを持たず、最初のbundleを受け取るときに作る。bundleはsourceごとに1つで、親は、workerが `blob_verify` で答えた「cloneが履歴ごと持っているcommit」（`have`）より下を除いて作る。`have` が空なら、execution commitの全履歴を入れる（第11章）。`have` はworkerが決める値なので、親は形式の合わないものを捨て、256個までしか見ない。親のstoreに置くbundleのファイルは `have` の集合ごとに1つで、1つのsourceにつき4つまでとし、超えた分は古いものから消す。workerがそのsourceを持った（`blob_verify` の `ready`）とき、または転送をあきらめたときに、そのsourceのbundleのファイルを全部消す。親は1つのworkerと1つのsourceについて、bundleを最初の1回と `bundle_base_missing` を受けての作り直し2回までしか作らず、それでも合わなければその転送を失敗にする。（このうち、sourceごと4つの上限はstoreが守る。`ready`・あきらめたときの削除と作り直しの回数の上限は、親の送信側が守る規則で、送信側はまだ実装していない。）未pushのHEADも実行できる。submoduleとGit LFSは、固定revisionと実体をmanifestに含める。dirty submoduleは再帰capture、LFS payload不足は実行前errorとする。

worktreeのraw bytesとGitのcanonical blobを区別する。LFS、clean／smudge filter、改行変換を経た実際のmaterialized fileがmanifestのhashと一致することを検証し、capture済み内容へfilterを重複適用しない。pathはlosslessに扱い、shell文字列連結やlossyな文字コード変換で収集対象を変えない。

symlinkはlink値を保持し、取り込み時にrepo外を自動追跡しない。repo外入力が必要なら入力として宣言して相対mountへ配置する。FIFO・socket・device等の非通常ファイルは明示errorにする。source／artifactの展開では絶対path、`..`、symlink traversalによる管理領域外への書込みを拒否する。

## workerでの準備

workerはrepoごとのcanonicalなlocal cloneと、attemptごとの隔離workspaceを持つ。親の絶対cwd、`.git` のworktree参照、準備済み `node_modules`、venv、絶対symlinkをそのまま移送しない。依存はworker自身のprofileで準備する。

Git外のgeneric execにはdirectory snapshotを提供する。sourceが不要な管理コマンドも明示できるが、通常実行から黙って入力を省略しない。元の手元ディレクトリを直接更新する用途には `exec --in-place` を用意し、親local workerだけを適合候補とする（`--local` を含む）。通常の分散実行で、生成物を元worktreeへ自動で上書きしない。

`in-place` は明示的な別workspace modeである。記録するsnapshotは比較用baselineで、実行入力の不変性は保証しない。remote配置、結果cache、既定の自動retryを無効にし、表示・結果でもsnapshot実行と区別する。

`exec --in-place` はGit worktreeとGit外directoryの双方を対象とする。Gitでは元worktreeのrootを保持し、元cwdからの相対pathでcommandを実行する。親local workerのidentityとrun/source/manifest digest、canonical rootのdevice/inodeを親state内の0600 bindingに保持し、rootの差替えや別workerへの配置を拒否する。原本pathは転送payloadや公開queryへ含めず、task/completionの `in_place: true` でmodeを示す。setup templateの適用・builder起動を行わず、prepare commandsは原本で通常commandとして実行する。workspace cleanupで原本を削除しない。

明示 `retry` は通常と同じterminal・停止確定・回収・control revisionの条件を満たした場合だけ、同runの新attemptとして同じ原本を再実行する。baselineは作り直さない。元のsecret globs契約はprivate bindingに保持し、現在のproject configを再loadしない。原本secretはbounded/no-followのread-only処理で実行時の値をmaskし、bundleの配置・chmod・消去を原本へ行わない。元globsと現在の `.worktreeinclude` が選ぶpath集合がbaselineのsecret inventoryと違えば実行・retryを拒否し、新しい `exec` によるbaseline作成を求める。値の変更だけなら現物をmaskして実行でき、元Vault bundleの消去は妨げにならない。例外は1つで、CIのsplit Jobのfail-fastが一度も配置しないままcancelしたShardは、停止を証明するattemptが無くてもretryできる（何も実行されていないため）。attemptを持たないそれ以外のtaskを含むretryは断る。

private bindingはowner再起動と明示retryのため親stateとともに保持する。binding自体はbaseline sourceや結果のGC pinではなく、元sourceが消去されたrunはretryできない。提出を断ったrunのbindingはその場で消し、提出されなかった（放棄された）取り込みのbindingは、その取り込みと一緒に消す（25.4の (b)・(c)）。受理されたrunのbindingは、runの刈り取り（25.5の段5）で消す。bindingやrunの取消・workspace cleanupは原本の削除を意味しない。

依存の準備は、プロジェクトの `.fast-worktree.toml` の `[[clone]]`（と `post_add`）が担う。attempt workspaceをfast-worktreeで作る方針と、`.worktreeinclude` の扱いは22.5で定める。第8章のSetupRecipeとtemplateは、workflowのsetup stepの再利用に限る。`.fast-worktree.toml` を持たないプロジェクトでは、第8章の `[prepare]` が代わりを務める。両方がある場合は `.fast-worktree.toml` を優先し、`ci plan` が準備の出所（`fast-worktree` / `[prepare]` / workflowのsetup step）を表示する。

# 7. CI計画と分散単位

## FrozenPlan

親は入力snapshotに対して一度だけ計画を作り、`FrozenPlan` として固定する。workflow選択、無効化の判断、config、差分、branch、base、job filter、Job DAG、ExecutionTask／Shard、依存、実行条件を含める。workerはこの計画を実行し、同じ引数で `local-ci run` を再度解釈し直さない。

実行中のneeds outputによるdynamic matrixや条件式は、依存出力が得られた段階で評価する。提出時点に全variantの値が分かっているとは扱わない。展開結果を永続eventとして固定し、再接続で別の展開を行わない。

## Job DAG・ExecutionTask／Shardと配置の境界

現行local-ciのUnitは、needs関係や実行環境を共有するJobをまとめている。新しいplannerはUnitを唯一の配置単位とはせず、Workflow → Job DAG → ExecutionTask／Shard の階層を作る。`needs` は同一ホストへの固定ではなく、先行Jobの結果・output・artifact確定を要求する依存辺とする。Job間のworkspace共有はGitHub Actionsの通常の独立Job意味論に従い、暗黙のファイル共有を仮定しない。必要なデータは明示artifactまたは契約化したimmutable inputとして渡す。中間artifactの範囲は、同一run内の同じworkflow内に限る（第13章）。worker側で `-j` を再解釈して先行Jobを重複起動しない。

**未配置のJob／Shardは動的に配置できる。配置された実行セッション（workspace・service・step状態を持つ単位）は開始後、同じhostに固定する。** `needs` の先行Jobが別hostで完了しても、結果と必要artifactが親で確定した時点で後続Jobを別workerへ配置できる。同一Jobの任意の連続stepを、workspaceの状態転送なしで別hostへ移動しない。受理済みだが未開始のattemptを再配置する場合も、元workerのretireと停止確認を要する。

run開始時に全Unitをworkerへ押し込まない。親に未配置backlogを残し、workerごとの先行受理枠を制限する。既定の `prefetch_per_worker = 1` は、実行セッション開始前の先行taskを1件に制限する値であり、実行中step数やCPU budgetとは別である。実際のstep並列数はtask-traffic-controllerのadmissionとCIのconcurrency規則に従う。

### 分割モデルと決定規則

`FrozenPlan` は `JobGraph`（workflowのJobとneeds辺）と `ExecutionTask`（配置可能な実行セッション）、`ShardGroup`（分割元Jobと集約契約）を持つ。各ExecutionTaskのID、実行条件、必要input／artifact、workspace policy、サービス、資源見積、retry policy、出力検証を固定する。Jobの内部stepは原則同一ExecutionTaskに置く。runner adapterが入出力の独立性を証明できるテスト部分だけ、元Jobを複数ExecutionTaskへ展開できる。

分割可能性の判断は「runnerがテスト集合を列挙できる」「各Shardで正しく対象testを指定できる」「Shard間で共有状態を競合更新しない」「必要fixture／DB／serviceを独立提供できる」「集約結果が元のJob判定と一致する」の全条件とする。単にscript名に `test` が含まれるだけでは分割しない。splitできない理由は `belt ci plan` に出す。

### 自動shardingの実装契約

初回版はJest・Vitest・Playwright・Cypress・`cargo test` のうち、対象プロジェクトで実際に使うrunnerをadapterで認識する。adapterは `discover`、`partition`、`invoke`、`collect`、`merge` を備え、runnerのversion・設定・filter／tag／skip条件を保持する。サポート不能な機能を推測で分割せず、安全な単一ExecutionTask実行へfallbackする。選択されなかったtestや失敗したtest discoveryも明示する。

partitionは各テストファイル（またはrunnerの安全な分割単位）の履歴時間を使い、予測完了時間の長いものを優先して、worker性能補正後の各Shardの推定負荷が均等になるよう割り当てる。初回実行や履歴不足時は安定した名称順と決定的hashによるfallbackを使う。同じsnapshot、runner version、shard plan、履歴snapshotなら同じ分割になる。極端に小さいtaskはsetup・転送の固定費を考慮してまとめる。shard数はworkerの実効容量、CI全体の残りDAG、並列上限に応じて決定する。

同一テストが異なるShardに重複しないこと、対象全件を一度だけカバーすることをmanifestの集合検証で保証する。テストの動的生成などで事前列挙が不完全なrunnerではrunner固有の安全なshard機能だけを使う。DBを共有するE2Eはdatabase／schema／port／containerをShardごとに隔離できないなら分割禁止とする。

### 結果集約・失敗・再試行

Shardの結果は `job_id / shard_id / test_identity` で集約し、assertion数、failed／skipped test、coverage、JUnit等のartifactを重複・欠落検証する。coverage mergeはrunner対応方式だけで行い、単純な数値加算で代替しない。元Jobの `needs` outputと後続の開始判定は、ShardGroup全体が終了し集約が確定した後に公開する。fail-fastは親が残りShardへのcancel要求を発行し、停止確認と中途結果を残す。retryは失敗Shardを同じsnapshotで新attemptとして実行し、通過済みShardの検証済み結果を保持できる。ただしrunner固有の非冪等テストでは明示的なretry許可が必要。

beltの親の実装（S18）: split済みのShardGroupを持つFrozenPlanのShardを、依存を持たないtaskとして並べて配置する。全Shardが終わると `local_ci::complete::aggregate_shard_group` とoutputの一致でJobのoutcomeを1つ作り、後続taskのinputsにはそれだけを渡す（Shardの順で作り、到着順は使わない）。receiptはtaskとJobの名前が合うものだけを数える。全Shardが成功でも、欠落・重複・不一致が有ればJobはfailureになる。`fail_fast` のShardGroupでは、失敗したShardを見つけた進行のpassが、終わっていないShardのcancelを1回だけ要求する（未配置は理由付きのcancel、配置済みは対象attemptと理由を `ci_fail_fast_requested` に記録し、coordinatorがcancelを届ける）。retryは既存の `retry` で、成功したShardのattemptと結果を残し、それ以外だけを新attemptにする。retry対象の後続taskは再び保留し、親Jobの新しい集約結果をneedsに渡してから配置する。run単位のretryでは成功済みの後続taskも保持する。fail-fastで一度も配置されないままcancelされたShardはattemptを持たないが、retryの対象とし、cancelを外してqueueへ戻す。Jobのoutcomeは集約した時点でShardの行に残し、runの結果はこれを読む。集約前のrunは確定せず、集約がfailureならattemptが全部成功でもrunは失敗（exit code 1）になる。`ci`・`wait`・`show`・statusは同じ結果を読む。task単位の結果（`wait <task>` など）はShard自身のattemptの結果で、Jobの集約を待たず、集約のfailureも反映しない。runの失敗の原因は、fail-fastで止められただけのShardではなく実際に失敗したtaskとする。単独でcancelされた別Jobのtaskも原因にせず、止められたShardやそのtaskのIDが先に並んでも、runは失敗したShardのexit codeを返す。run全体をユーザーがcancelし、そのcancelがまだ終わっていないShardを止めた場合は、Shardのreceiptに失敗したtestが有っても（attemptが失敗でも成功でも）、Jobの集約がfailureになっても、taskのIDの並びに関わらずrunはcancelled（exit code 130）で終わる。fail-fastが残りを止めてrunが失敗で確定した後に届いたcancelは、結果を変えない（cancelの要求時刻をrunのcancelの時刻と比べる。同じミリ秒ならrunのcancelが止めたものとして扱う）。completionの `cancel_requested`、最初の失敗、exit codeは同じ結果を示す。retryは、attemptが成功でも単独で通過と言えないShard（receipt無し、testの失敗、渡されたtestの報告の過不足）をやり直す。この印はretryがrunの全taskを読み終えてからJobのoutcomeと一緒に外すので、taskの数が走査の1ページを超えても同じShardを選ぶ。`ci` はShardごとの行とJobの行を出す。複数の実行枠がある場合、本番の `ci` は候補Jobの通常taskを保留し、`ci_discovery_v1` を交渉したworkerでsetup・runner version確認・列挙を行う。`needs` を持つJobの探索には、上流Jobの集約後に得たoutputを渡す。親は列挙結果と元planの対応を検証し、同じsnapshotを参照する確定済みの分割を永続化する。列挙不能、または応答した候補workerがすべて探索機能に非対応なら元のJobを実行する。matrixを含むrunでは、SDKの再生可能なjournalを開始する前に、独立したJobの列挙結果を確定する。確定したShardの実行workerは、setup後の実行ディレクトリでrunner versionを再照合する。不一致・取得失敗時はテストを開始せず、stepログに理由を残して失敗させる。`ci plan --run <run-id>` は保存済みplanの分割と件数を示す。

fail-fastで、すでに動いているShardを止めたときは、止める理由（`ci.unrunnable`）をそのtaskのspecに、cancelの要求と同じtransactionで残す。理由は `show <run>` のtaskの `unrunnable` に出る。配置前にcancelしたShardと同じ形である。retryでtaskを開き直すと、理由は消える。止める要求を出した直後にそのShardが成功した場合も、理由は残る（既知の制限）。

retryは、失敗したJobだけでなく、その失敗のために `skipped` になった後続のJobも開き直す（runのretryでも同じ）。規則は次のとおりとする。
- 後続のtaskが、stepを1つも実行せずに終わっていた（Jobがすべて `skipped`）ときは、入力にした結果が古くなるので、そのtaskを開き直す（generationを上げて保留にする）。進行が、新しい結果で走らせるか、改めて決める。そのtaskを必要とする後続も同じ見方でたどる。
- 後続のtaskが、始まる前に系によってcancelされていた（`ci.unrunnable` の理由が付いている）ときも、開き直して保留に戻す。利用者がcancelしたtaskは、cancelのままにする。
- 後続のtaskが実際に走っていた（実行中、またはstepを実行して終わった）ときは、Job 1つだけのretryを拒否する。入力が古くなるためで、エラーの文言で、その後続taskも含めるか、runのretryにするよう案内する。runのretryでは、走ったtaskは残す。
- 結果を読めない後続taskは、走っていないと言い切れないので、走ったものと同じに扱う（Job 1つのretryは拒否し、runのretryでは残す）。
- 開き直したtaskは、retryの `task_count` に数える。
- discoveryで適用したtaskと、動的に生まれたtaskは、この対象にしない。Shardの開き直しは、方針がretryを許すときだけ行い、許さないときはretryを拒否する。

履歴はプロジェクト、workflow、Job、作業ディレクトリ、runner versionと実行設定で区別する。成功かつ全テストを確認できたShardのreceiptから、実行workerと各テストの実測ミリ秒を保存する。同一テストを複数workerで実行した観測があれば実行時間の比の中央値を使い、基準workerでの時間に正規化してから分割する。分割時に想定したworkerは配置比較の最終同点で優先する。転送時間と繰り返すsetupの実測下限を固定費に使う。ブラウザーrunnerでは、対象Jobの隔離宣言と明示的なretry許可をcaptured configから読む。coverageを安全に統合できないJobは単一taskになる。coverageとJUnitの集約の範囲は次のとおりとする。assertion数の集約は未実装。

- **coverage**: 分割したJobで、Shardがcoverageを出す場合、親はShardGroupの集約（`aggregate_shard_group`）と同じ時点で、local-ciが公開するrunner対応のmergeを呼んで1つに合算し、Jobのcoverageとして後続のinputsと `outputs` に渡す。形式はlocal-ciのrunner adapterが定める。1つのShardでもcoverageが欠けている、またはmergeが失敗した場合は、全Shardが成功でもJobをfailureにする。mergeを持たないrunnerのJobは分割しない（上記）。
- **JUnit**: ShardごとのJUnitを `test_identity` で突き合わせ、同じtestの重複と、列挙したtestの欠落があればJobをfailureにする。

beltはcoverageの形式を自分で解釈しない。mergeの実装はlocal-ci側に置く。

単一Jobのステップ間で生成された書込み可能workspace、常駐service、プロセス内状態を自動移送することはしない。複数ExecutionTaskへ切り離す境界は、immutable artifactと明示依存だけで再現できる位置に限定する。

### workerでのCI taskの実行

CIのExecutionTaskは、通常のattemptのpayloadの一種（`ci_task`: FrozenPlanのdigest、task ID、needsの結果であるinputs）として配置する。commandのattemptと同じplacement・admission・lease・retry・結果回収の経路を使い、`hello` のfeature `ci_task_v1` を交渉したworkerにだけ送る（提供するが要求しない）。workerがこのfeatureを提供するのは、FrozenPlanの本体を受け取れるbuildからである。受け取れないbuildは提供せず、交渉していない接続からのCI taskは `PROTOCOL_INCOMPATIBLE` で断る。FrozenPlanの本体はcontent-addressedなblobとして届ける。`blob_put` の種類 `plan` で、sourceの転送と同じ接続・同じ64 KiBのchunkで送り、runのsourceがworkerで使えるようになったあと、そのrunのtaskを配置する前に届ける（sourceとは別に保持し、sourceが揃っているかどうかに依らず受け取る）。この種類は `ci_task_v1` を交渉した接続にだけ送る。workerは全bytesが届いてdigestが一致したときだけ `<state_dir>/ci-plans/` に置き、digestが一致したbytesだけを使う。既に持っているplanをもう一度送られたworkerは、完了と答えて、そのplanを今届いたものとして扱う。workerが保持するplanは件数と合計bytesに上限を持ち、未完了のattemptが参照しなくなったplanは、保持期間を過ぎたらattemptのGCと一緒に消す。

workerではsupervisorがlocal-ci-coreのtask実行を呼び、coreが求める各stepを、commandと同じ経路（task-traffic-controllerの登録とadmission、launchの記録、mask済みのログ、timeout、停止と停止確認）でnodeとして実行する。coreはstepのprocessを起動せず、signalも送らない（service containerのdockerやgitなど、step以外の補助processはcoreが起動する）。taskの結果（Jobごとの結果とoutput）はattemptのartifact `belt-ci-task-result.json` として親へ返し、親が後続taskのinputsに入れる。この名前とその配下は予約で、CI taskのpayloadが同じ名前のartifactやoutputを宣言すると `INVALID_INPUT` で断る。後続taskのinputsの上限（4 MiB）を超えるoutputを持つtaskは、実行したworkerで失敗にする。

supervisorは、coreがstepの外で作業している間（workspaceの照合、stepとstepの間）も、commandの実行中と同じ間隔で取消要求・lease・attemptのdeadlineを見る。leaseの更新はstepの外でも反映し、止める理由が出たらcoreのcancel handleでcoreを止める（次のstepの要求を待たない）。attemptを終わらせるのは、cancel・lease失効・attemptのdeadline・資源待ちのtimeout・spoolの失敗・停止を確認できなかった場合だけで、その後は `if: always()` のstepも起動しない。stepやJobの `timeout-minutes` が切れた場合は、そのstepを止めて停止を確認したうえで、stepの結果（timeout）としてcoreへ返し、続けるかどうか（`continue-on-error`、`if: failure()`、`if: always()`）はcoreが決める。stepの実行時間の上限は、planの値とattemptの `command_timeout_ms` の小さい方である。stepがadmissionを待った時間は実行時間に含めず、待ち時間として別にcoreへ返す。

### 親でのCI runの進行

`ci` は入力を1回captureし、FrozenPlanを1回だけ作り、planのExecutionTaskをすべて1つのrunのtaskとして提出する。taskのIDは提出のときに決まり、あとから作り直さない。他のJobを必要としないtaskはそのままqueueに入る。他のJobを必要とするtaskは、同じrunのtaskとして受理されるが、inputsが決まるまで配置しない（待機）。待機中のtaskもrunの完了・cancel・表示の対象である。

親は、taskが必要とするtaskがすべて終わり、その結果の回収が閉じたときに、結果のartifactから下流のinputs（直接必要とするJobの結果とoutput）を作り、待機中のtaskに書き込んでqueueに入れる。この書き込みとcancelの確認は1つのtransactionで行い、先に来たcancelが勝つ。上流がどう終わったかに依らず下流は配置する。Jobを実行するかどうか（既定の `success()`、`failure()`、`always()`、その他の `if:`）は親では決めず、workerのcoreがinputsから評価する。実行しないJobは、stepを1つも起動せずに `skipped` として結果に残る。

上流が使える結果を残さなかった場合（cancelされて配置されなかった、attemptが結果なしで終わった、結果が読めない）は、親がそのtaskのJobを `failure`（cancelによる場合は `cancelled`）として下流のinputsに入れる。inputsは1 MiBまでとし（payloadは接続の1 frameに収める）、超えるtaskは実行できないものとして理由をeventに残してcancelする。

親が進められないrunは、黙って待たせない。次の2つを区別する。

- 進められないことが親の持つ状態から決まる場合（FrozenPlanのfileが無い、digestが一致しない、planとして読めない、runのtaskがplanのtaskと対応しない、進行の規則が入力を拒む）は、何度見直しても同じなので、そのrunのまだ配置されていないCI task（待機中のものと、queueにあるもの）をすべてcancelする。理由はtaskのspecの `ci.unrunnable` に書き、taskごとにevent `ci_task_unrunnable`（`task`、`reason`）を残す。理由の書き込み・cancel・eventは1つのtransactionで行う。すでにattemptを持つtaskには触れず、そのまま終わらせる。runは成功にならずに完了する。inputsが上限を超えたtaskも同じ手順でcancelする。親がplanを読めないことは、workerへplanを届けるとき（待機中のtaskを持たないrunを含む）にも同じ扱いにする。
- その時だけ読めなかった場合（親のDBが使用中、planのfileの読み取りがその他の理由で失敗）は、runを変えずに次のheartbeatでやり直す。理由はrunのevent `ci_run_stalled`（`reason`）に、障害が始まったときと理由が変わったときに1回だけ残す（親のprocessの中で数えるので、再起動すると同じ理由をもう一度残すことがある）。

cancelした理由は、`show`・`ls` のtaskの `unrunnable`、`logs` のそのtaskのstatus（`reason`。followの途中でcancelされた場合も、理由が付いたstatusをもう一度出す）、`ci` のJobごとの結果（`reason`、textでは `no_result (理由)`）に出す。

workerがFrozenPlanを受け取らなかった場合（保持できるplanの件数・合計bytesの上限、途中のfileの上限に達していて `blob_put` を断った。workerがerrorを答えた場合に限り、接続の失敗は含めない）は、そのworkerではrunの入力がreadyにならない。親はそのworkerを、runのtaskの配置から理由付きで外す（`input_refused`、理由は `unmet`）。ほかのworkerがplanを受け取っていれば、そこへ配置する。外したことは配置の判断に残り、どのworkerにも配置できない間は `task_waiting` に残るので、`show` のtaskの `placement` と、`logs` のそのtaskのstatus（`placement`。followの途中で変わったらもう一度出す）に出る。runにはevent `ci_plan_refused`（`worker`、`reason`）を、接続ごと・理由ごとに1回残す。配送は同じworkerへ繰り返し試し、workerが受け取った時点（GCで空きができた後など）で除外は終わる。workerが別のboot・agent epochになったときも終わる。この除外は一時的なもので、`worker_cannot_run` の記録は作らず、`blocked_reason` も付けない。

CI taskの入力（sourceとFrozenPlan）をworkerへ届けるたび、配送が成功した場合に、親はrunのeventとして `ci_input_delivered` を記録する（payload: `worker`、`source_was_ready`、`source_ms`、`plan_ms`）。配送が失敗したとき、配送の途中で停止を受けたとき、試行用（trial）の状態では書かない。eventの書き込みに失敗した場合は、`event ci_input_delivered not written: <原因>` の1行をstderrに出す。書き込みの失敗は配送の成否に影響しない。

進行の状態は親のprocessに持たない。runのtaskの行、FrozenPlanのfile、回収済みの結果のartifactから毎回組み立て直すので、親が再起動しても同じところから続き、同じtaskを二度作ることも二度配置することもない。進行の規則は、local-ci-coreの共有driverと同じ形（入力を渡し、保留中のactionを実行して確認する）のtraitの後ろに置く。matrixの展開・permit・max-parallel・fail-fast・集約は共有driverに置き換えてから扱い、それまで、matrixのvariant・式のmatrix・shard・service containerを持つplanと、secret_filesを持つsourceのCI runは、提出の時点で理由を付けて断る。

`ci_task_v1` を交渉していないworkerにはCI taskを配置しない。除外したworkerと理由（`protocol feature ci_task_v1`）は配置の判断に記録する。配置のときのforecastには、planがそのtaskについて挙げるcommandの数だけのnode（`step-N`、実行の順）を渡す。最初のstepだけをtask全体として尋ねない。

CI taskの結果のartifactは、payloadが宣言できない名前だが、親は必須（required）として回収する。無い場合と、そのtaskの結果として読めない場合は、回収を `required_missing` で閉じ、commandが成功していてもtaskを成功にしない。CI taskが宣言できるartifactとoutputは、結果の1件を除いた数までである。

service containerを持つtaskは受理しない（supervisorが失われたとき、coreが起動したcontainerを回復で止める仕組みがまだ無いため。理由を付けて `INVALID_INPUT` で断る）。

coreはworkspaceがcaptured sourceとbyte単位で一致するときだけ実行を束縛する。そのため、secret_filesを配置するsource、prepared setupを伴うattempt、in-placeのworkspaceではCI taskを受理しない（local-ci-core側に、宣言済みの上書きを除いて照合する束縛を足して解く。決定と制約は docs/plan.md のS16節）。

## matrix・自動テストsharding・明示shard

独立workspaceと明示input／outputで実行できるmatrix variantやtest shardは、初回実用版から複数workerへ分散できるようにする。共有workspaceの任意のfile依存を自動的に推論して切断する仕様にはしない。

分割を許可する条件は、plannerが独立性を扱える構造であること、またはプロジェクト設定で独立workspace・入力・出力・集約方法を宣言していることとする。前段の生成物が必要な場合はimmutable artifactとして確定してからvariantへ配る。後段は必要なvariantのterminal結果とartifactが揃うまで開始しない。

親が全workerを通した `max-parallel`、`fail-fast`、variant ID、結果集約を管理する。どのvariantのoutputを後段へ渡すか曖昧なら、明示した集約規則を必須とする。既存の共有workspace前提のUnitは維持する。

`belt ci plan` は、分散可能なtask数、依存関係、並列幅、分割できない理由を表示する。実利用するCIが1つの不可分Unitに閉じている場合、そのまま「複数Mac対応完了」と判断せず、独立shardの契約を設計する。

# 8. setupとcache

各配置可能ExecutionTask／Shardは、他hostのsetup成功に暗黙依存せず、自分の準備を完結できる計画を持つ。同一Jobの分割Shard間で必要な入力はimmutableなprepare receipt・artifactとして渡し、共有の書込み可能workspaceを仮定しない。

現行の全体hoist後の `execute=false` や、別jobがsetup所有者であるという参照を、そのまま分散用DTOへ保存しない。元stepを保持したUnitから `SetupRecipe` を導出し、実際に準備されたstepの対応だけを `PreparedTemplate` receiptに記録する。workerはそのreceiptにあるstepだけをskipする。

template keyには、source digest、順序付きsetup recipe、working directory、解決済みshell、workflow／job／stepを合成した実効env、toolchain・OS・arch・profile、cache schema、reuse scope、freshness policyを含める。scopeは `run / persistent` を区別し、run scopeではrun IDもkeyへ含める。コマンド文字列だけが同じでも準備の同一性を認めない。

同じtemplateの生成はworker内でsingleflightにし、複数Unitで待てる。builderは独立した `builder_id` とconsumer参照集合を持つ（workerでは参照集合をagentのプロセス内に持つ。下記）。1つのUnitのcancelや再配置で、他Unitが必要とするbuilderを止めない。最後の有効参照がなくなった場合にだけbuilder停止を判断する。setupも重いcommandとしてtask-traffic-controllerに登録し、builder IDに対するbudget chargeとforecast上の需要は1つだけ保持する。

完成templateはimmutableとし、Unitごとにcopyまたはwrite-isolatedなcloneを作る。書込み可能なhardlinkの共有は禁止する。新しいrequirementsを既存templateへ追加実行して上書きせず、別keyで構築する。forecastでは共有builderの負荷を1回だけ数え、各Unitの依存として参照する。

workerでのtemplateとbuilderの実装は次のとおりとする。
- **templateの中身**: builderは、stepの実行前と実行後にworkspace全体を読む（全ファイルの内容をdigestで比べ、更新時刻やサイズだけで「変わっていない」と判断しない）。その差分、つまり追加（深さを問わず、新しく現れた最上位のentry）、変更（元からあるファイルとlinkで内容が変わったもの。変更前のdigestと変更後のdigest）、削除（変更前のdigest。ディレクトリは空の場合だけ消す）をreceiptに記録し、追加と変更の中身をtemplateに保存する。stepがどこへ書いても結果は変わらない。consumerのworkspaceはsourceから普通に作ったうえで、この差分を適用する。先にすべてを照合し、追加先に別のものがある、変更・削除の対象がbuilderの開始時点と違う、途中のpathがlinkである、のいずれかなら何も書かずにそのattemptが自分でstepを実行する。copyはworkspaceの外（templateの置き場のscratch）で作ってreceiptと照合し、renameで適用する。適用の途中で失敗したら適用済みの分を元に戻す。元に戻せなかった場合だけ、そのattemptをエラーにする。差分が空のtemplateも有効で、stepが成功しworkspaceを変えなかったことの証明として扱う。
- **templateにしないもの**: 次の場合はstepが成功してもtemplateを公開せず、consumerに理由を返して各自でstepを実行させる。同じkeyで作り直しても結果は同じなので、workerは理由をプロセス内に覚えておき、次のconsumerにはbuilderを起動せずに返す（再起動で忘れる）。(1) `.git` という名前のentry、またはその下への追加・変更・削除、および追加されたディレクトリの中の `.git` ファイル。`git submodule update --init` が該当する。runnerが作るworkspaceの `.git` は別の場所のrepositoryを指すファイルで、repository本体はworkspaceの外にあり、templateに入れられない。追加されたディレクトリの中の `.git` ディレクトリ（その中で完結したclone）は通常のentryとして保存する。workspaceのrootに `.git` ディレクトリが新しくできた場合（`git init`）も公開しない。元からあるrootの `.git` ディレクトリ（in-placeのworkspaceだけが持つ）の中は読まない。(2) workspaceの外を指すlink（絶対pathで外を指すもの、相対pathで外へ出るもの）。builderのworkspace内を指す絶対pathのlinkは、公開前に相対pathへ書き換える。workspace内にとどまる相対link（workspace installの `node_modules/a -> ../packages/a`）はそのまま保存する。(3) ディレクトリがファイルに、またはファイルがディレクトリに変わった場合。(4) 追加が256件、変更と削除の合計が4096件を超える場合、深さが128を超える場合、ファイル・ディレクトリ・link以外のentryがある場合。stepがworkspaceの外に書いたもの（`$HOME` のcacheなど）と、ファイルの中に書かれたbuilderの絶対path（`pip install -e` の `.pth`、venvのshebang）はtemplateでは扱えない。in-placeのworkspaceにはtemplateを適用せず、builderも起動しない。
- **keyの算出**: keyはworkerが算出し、wireのkeyは信用しない。os・arch・profileはworker自身の値を使い、実行するstepのid・dir・script・shell、stepが実際に受け取る実効env（workerがstepに与えるenvにstepのenvを重ねたもの）、toolchainの宣言、宣言されたtoolの実体（stepの `PATH` で解決した実体の絶対path・サイズ・更新時刻。無ければ「無い」）、cache schema、scope（run scopeではrun ID）、freshness、secret versionを含める。`--no-cache` と `[ci] no_cache` はscopeをrun scopeにする（他runのtemplateは使われず、同じrun内のsingleflightは残る）。toolの実測version（S20）はまだkeyに入らない。version managerのshim経由のtoolと、宣言されていないtoolの更新はkeyに現れないので、それまではpersistentなtemplateがtoolの更新より長く残りうる。避けるにはfreshnessを指定する。
- **先に準備するstep**: 先頭から数えて、実行時にだけ値が決まるenvを持つstepと、workerが実行しないshell（bashとsh以外）のstepより前までとする。それ以降のstepはconsumerに残す。secretを宣言したsetupは1つも先に準備しない（builderにはsecretを渡さないため、consumerと同じ条件で実行できない）。
- **builder**: builderは、template keyごとに1つ、worker内部のattemptとして `<state>/setup/builders/<builder_id>/` の専用DBに記録し、通常のattemptと同じ `supervise` プロセスで動かす。stepはtask-traffic-controllerのadmissionを通り、leaseは専用で、consumer参照が1つ以上ある間だけworkerが更新する。最後の参照が外れると更新をやめてcancelする。停止はsupervisorが行う。builderが失敗・timeout・cancel・消失したときは、待っていた全consumerに理由を返し、consumerは自分でstepを実行する（失敗は記憶せず、次のattemptが作り直す）。consumer参照はagentのプロセス内だけで持ち、永続化しない。builderはagentの再起動をまたいで生き残らない。再起動時に前回のbuilderは参照の有無にかかわらず止めて片付け、templateを必要とする次のattemptが作り直す（作り直しは常に安全である）。supervisorが消えたbuilderをagentのプロセス内で片付けるときは、先にcancelを記録する。起動を1つも記録していないbuilderのstepをagentの中で実行しないためである。
- **検証と破棄**: consumerへのcopyを、適用する前にreceiptのtree digestと照合する。一致しないtemplateは使わずに破棄し、次のconsumerが作り直す。破棄は、照合したreceiptのdigestが今もそこにあるtemplateのものと同じ場合に限る（その間に別のbuilderが公開し直したtemplateは消さない）。templateは完成後に書込み不可にし、hardlinkは使わない。公開のrenameの前に、ファイルとディレクトリをdiskへ書き出す（fsync）。templateを置き場から外すときは、先に別名へrenameしてから消す。copy中に外されたconsumerは読み取りに失敗し、自分でstepを実行する。
- **templateの削除**: templateごとに最後に使った時刻とサイズを記録する。合計が上限（既定20 GiB）を超えたら、最後に使った時刻が古い順に消す。上限の日数（既定14日）使われなかったtemplateを消す。run scopeのtemplateは、そのrunのattemptがworkerに無くなってから一定時間（既定1時間）使われなければ消す。copy中のtemplateは消さない。削除はbuilderの終了時、templateの適用後、agentの起動時に行う。
- **全commandが準備済みのattempt**: templateがattemptの全commandを満たした場合（installだけのtask）、commandを1つも起動せずにsucceededで確定する。resultのexitは無し、logは空、宣言されたartifactはworkspaceから通常どおり集める。eventに、起動しなかった理由と省いたstepを記録する。cancelが記録されていればcancelledになる。
- **受理時の検査**: SetupSpecの上限と形式は受理の時点で検査し、違反はINVALID_INPUTで返す。
- **起動時の掃除**: 途中のtemplate（scratchディレクトリ、receiptの無いディレクトリ）と、前回のbuilderを片付ける。supervisorが生きているbuilderには停止を依頼し、そのworkspaceとディレクトリは残して、止まったことを確かめてから消す（確かめられるまで繰り返し見に行く）。コマンドが起動済みのbuilderはsupervisorのrecoveryで止めてから消す。
- **`[prepare]`**: `.fast-worktree.toml` が無いときだけ、`commands` の各argvを1つのsetup stepに変換し、同じbuilderの経路で実行する。

cacheはworker内を基本とし、別hostの準備済み依存を無条件には流用しない。source、plan、profile、OS・arch、tool version、env参照version等が一致することを確認する。secretが結果へ影響する場合、値をcache keyに保存せずsecret version等で識別し、それができなければcache対象から除外する。

`--no-cache` は他runのprepared templateと計算結果のreuseを止める。プロジェクト設定 `[ci] no_cache`（`.local-ci.toml` の `no-cache` と同じ書式）に挙げたworkflowまたはjobには、`--no-cache` を付けた場合と同じ扱いを常に適用する。同じrunで必要な同一recipeのsingleflight共有は維持する。検証済みのimmutable source objectsは転送最適化として再利用できる。worker追加直後のcold setupも配置コストに含め、cache hitを前提にETAを表示しない。

runtimeのneeds、matrix、service、step outputに依存する準備は、その実行位置に残す。`GITHUB_ENV` 等の副作用をcapture・replayできないstepもhoistしない。venv等の絶対pathに依存する環境を別pathへ単純copyせず、安全な共有ができなければUnit専用に準備する。再利用可能性を確認できないsetupはrun内の再利用に限定する。最適化できないことを理由に必要なstepを省略しない。

本章のSetupRecipeとtemplateが扱うのは、workflowのsetup stepの再利用である。依存そのもの（`node_modules`、`.dart_tool` 等）の準備は、22.5のとおりfast-worktreeの `[[clone]]`（と `post_add`）が担い、worker用の準備設定を別に持たない。

generic execの依存準備もプロジェクトに宣言できる。`.fast-worktree.toml` を持たないプロジェクトでは、例えば `[prepare]` の `commands = [["corepack", "pnpm", "install", "--frozen-lockfile"]]` を、同じsetup・admission・cacheの経路で実行する。`.fast-worktree.toml` があるプロジェクトでは `[prepare]` より `.fast-worktree.toml` を優先し、`ci plan` に出所を表示する。`init` は検出したlockfileに応じた設定案を生成できる。`exec pnpm test` が別Macの偶然の `node_modules` に依存する構成にはしない。

# 9. 配置と待ち時間の見積もり

## 配置の手順

coordinator内の1つのscheduler actorが、配置のcommitを直列化する。複数利用者からの投入やworkerへの見積照会は並行してよい。

1. 親のready queueから対象taskを選ぶ。通常優先度では投入順と待機時間を考慮し、依存未解決の仕事で独立した後続を塞がない。
2. OS・arch・profile・tool・必要disk・明示resource要件で候補を絞る。
3. 各workerへ、現在のキューと候補workloadを含むforecastを照会する。
4. source差分転送、setup（第22章のtoolchain導入を含む）、実行、成果物回収を含む予測完了時間で比較する。
5. workerとattemptを選び、親DBに配置と未ACKの需要を同じtransactionで記録する。
6. transactionを閉じてからworkerへ送信する。次の配置はこの未ACK需要も考慮する。
7. workerの受理・登録・終了に応じて仮需要を実状態へ置き換える。

最短の予測完了時間を基本とし、同程度なら待機の少なさ、cache状態、転送量、既存affinity、stable worker IDで決める。これは推定に基づく配置であり、全体の最適解や完了時刻の保証ではない。

比較と同点の規則は次のとおりとする。

- 予測完了時間は、workerのforecastの答え（転送とsetupは「readyになるまでの遅れ」としてworkerへ渡すので、既存のキューと重なる）に、回収の見積を足した値とする。
- 「同程度」は、比較値が最小の比較値から1秒以内（1秒ちょうどを含む）であることをいう。同程度のworkerの中では、priorityが高い方、forecastの待ち時間が短い方（不明は最後）、親がそのworkerに置いているtaskが少ない方、cacheが温まっている方（温・不明・冷の順）、転送量が少ない方（不明は最後）、affinityがある方、worker IDが小さい方の順で決める。同程度の中では、比較値の大小そのものは順位に使わない。
- forecastが時刻を返さず `fallback` のranking（`target.ranking.finish_in_ms`）だけを返したworkerは、その値で比べる。`measured` の時刻と `fallback` のrankingは同じ尺度で比べ、配置理由にはどちらであるかを残す。rankingを時刻として表示しない。
- 数値をまったく得られなかったworker（forecastの時刻もrankingも `null`、errorの答え、期限切れ、boot／agent epochの変化、聞いたものと違うworker／planへの答え）は `unknown` とする。`unknown` を空きとは扱わない。数値のあるworkerが1台でもあれば、`unknown` のworkerは選ばない。全員が `unknown` のときに限り、上の同点規則だけで選ぶ（配置は止めない）。task-traffic-controllerは、登録済みのnodeを持つworkerについて数値を返せないことがあり、そのworkerはこの `unknown` になる。
- 親の側の見積（転送・setup・回収）がworkerによって不明のときは、その部分について候補の中で最も大きい既知の値を仮に入れて比べ、仮の値であることを配置理由に残す。全員が不明の部分は比較に入れない。
- workerへ渡す「readyになるまでの遅れ」は、転送の見積とsetupの見積の和とする。上の補完のあとでも、候補のどのworkerについても転送かsetupのどちらか一方が不明のときは、不明の方を0として和を取る（setupの所要が未知なら、そのsetupは0として扱う）。両方が不明のときに限り、遅れを渡さず（`external_delay_ms` を付けず）、不明のままにする。
- forecastを制限時間（1秒。設定fileのkeyではなく実装の定数で、`forecast_ttl_ms` より短くなければならない。短くない組み合わせではschedulerを起動しない）までに答えなかったworkerは、そのtickの残りの配置から外す。他のworkerの配置はそれを待たない。
- tickの途中でattachされたworkerは、次の1件の決定の前にほかのworkerと同じ確認（`inspect` と、そのworker上のattemptの確認）を受け、その決定から候補に入る。
- 決定ごとの配置理由（比べたworker、それぞれの予測と内訳・出所・補正量、勝った理由、外したworkerとその理由）は、配置と同じtransactionで親DBに記録する。どのworkerも選べなかったときの理由は、runのevent `task_waiting` に記録する。同じtaskについて書き直すのは、結論か、tickをまたいで続く除外（そのtaskを断ったworker、drain中のworker、能力が合わないworker、runの入力を受け取らなかったworker）が変わったときだけとする。そのtickだけの事情（満杯、forecastやsubmitに答えなかった、確認できなかった）が入れ替わっても書き直さない。決定に付ける番号 `decision_seq` は親processの中での順番で、再起動すると1から数え直す（DB上の識別子ではない）。
- 配置は古いtaskから順に、1回のtickで決まった件数まで見る。attach中のworker全員から外されたtask（下の `worker_cannot_run`）は、その順番の最後に回す。そのようなtaskが何件待っていても、配置できるtaskが後回しにならない。外していないworkerがattachされると、元の順番に戻る。

利用者は設定でworkerに好みを付けられる（`[[workers]]` の `priority`、親のlocal workerは `[coordinator] local_worker_priority`。整数、既定0、大きいほど優先、範囲は -100〜100。範囲の外の値は近い方の端として扱う）。比較値は次のとおりとする。

```
比較値 = 予測完了時間 + (候補中の最大priority − そのworkerのpriority) × priority_step_seconds
```

`priority_step_seconds` は `[scheduler]` の設定（整数秒、既定60、0〜3600）。比較値が最小のworkerを選び、同程度の判定と同点規則の先頭にpriorityを置く（priorityが高い方、次に上の既存の規則）。手順2の能力条件（requires・labels）による候補の絞り込みは従来どおり強制で、priorityは候補の中での好みに限る。全workerのpriorityが同じなら補正量は0になり、従来と同じ配置になる。ephemeral workerもpriorityを持てる。

例：`local_worker_priority = -1`、他のworkerが0なら、手元は60秒遅いものとして比べられ、他のworkerが60秒以上遅れない限りそちらへ配置される。手元を実質使わないなら大きな差（例：-50）を付ける。配置理由の表示には、priorityによる補正量（worker別の秒数）を含める。

workerはactive task数と未開始prefetch数の両方を制限する。既定は `max_active_task = 2`、`prefetch_per_worker = 1` とし、設定で調整できる。resource待ちのactive taskもactive枠を使う。親は、あるworkerに置いたtask（開始済み・未開始・未ACK・unknown・retire確認待ちを含む）の合計が `max_active_task + prefetch_per_worker` に達したら、そのworkerへ新しく配置しない。上限は親の設定とworkerが `inspect` で答えた値の、項目ごとに小さい方を使う。件数は配置を記録するtransactionの中で数え直す。セッションだけを大量に開始して親のbacklogを消費する実装を避ける。step単位のCPU・メモリ制御は別途task-traffic-controllerが行う。

この受付窓は、そのworkerへ仕事を投げるすべてのメンバーに共通の1つの予算で、値は受ける側のマシン自身の設定が決める（`inspect` で答える）。投げた側が数えられるのは自分が置いたtaskだけなので、上限を守る責任はworkerにあり、workerは窓を超える新規submitを断る。断られた側は、retireを確認してから別のworkerへ置き直す（24.4）。投げた側の件数の確認は、断られるsubmitを減らすために残す。

## 読み取り専用の見積（`estimate`）

`estimate -- <command>` は未投入のcommandを、`estimate --task <ID>` は保存済みtaskの入力（commands・env・requirements・toolchains・出力計画・source）を使って見積もる（queueで配置を待っているtaskならその配置、それ以外なら仮の追加実行）。どちらも、既に起動している認証済みownerにだけ照会する。ownerを起動せず、停止中は `online: false` とし、実行・待ち・転送・setup・回収をすべてunknownと答える。

見積は読み取り専用である。親のqueue・task・attempt・配置・未ACK需要・待機理由（`task_waiting`）・coordinator epochを変えず、workerへは `inspect`、frozen toolchainの照合、forecastの読み取り照会だけを送る。workerのledger entry・lease・job・受理予約も増えない。実測の転送でcoldなworkerを選んでも、配置のときのprefetchは起こさない。何回実行しても同じである（A06）。

保存済みtaskの見積は、配置と同じ判断を同じ材料で行う。

- 親の見積（転送・setup・回収）は、親が実測した部分を先に使い、実測の無い部分だけcost modelの値で埋める。この合成は配置と同じ関数で行う。
- workerによって不明の部分には、候補の中で最も大きい既知の値を仮に入れる。これをforecastの照会より前に行い、workerへ渡す「readyになるまでの遅れ」を配置と同じ値にする。
- forecastの答えの年齢は、照会を送る直前から全員の答えが揃った時点までを測る。errorで答えたworkerは、配置がreadyなworkerをそう扱うのと同じく、数値の無い候補（`unknown`）として比べる。
- そのtaskを断ったworker（`worker_cannot_run`）、CI taskを受け取れないworker、drain中・満杯・能力が合わないworkerは、配置と同じ理由で外す。
- 比較・同点規則・priorityの補正は配置と同じ `decide` を使う。

queueで配置を待っているtask（配置が今拾える状態のtask）の見積は、続けて行う配置が実際に決めることを答える。そのため、runの入力についても配置と同じ条件を写す。

- sourceがreadyなworkerと、readyではないが親がそこへの転送を実測済みのworkerだけにforecastを照会する。どちらでもないworkerは照会せず、配置理由にも載らない。見積は前提の欄に「照会しなかった」と理由を示す。
- runの入力を受け取らなかったworkerは、配置と同じく理由付き（`input_refused`）で外す。
- attach中のworkerがあり、そのどれもsourceを持たず入力も断っていない間、配置はそのtaskについて決定をしない（待機理由も記録しない）。見積も決定を返さず（`decision` は `null`）、まだ決定が無いこととその理由を前提の欄に示す。
- sourceを持たないworkerは、使えて期限内のforecastの答えがあるときだけ比べる。errorの答えや期限切れなら、forecast無しとして外す。sourceを持つworkerは、errorの答えでも数値の無い候補として比べる。
- sourceを持たないworkerが選ばれたときは、配置がまずsourceをそこへ送り、readyになってから決め直すことを前提の欄に示す。見積はその転送を始めない。
- 外す理由を調べる順序は配置に合わせ、到達不能・drain中、満杯、入力の拒否、sourceの有無、能力、そのtaskの拒否の順とする。

したがって、同じ状態（同じworker・同じ負荷・同じ入力の状態・同じcost model・同じforecastの答え）なら、queueにあるtaskの見積が示すworker・勝った理由・比べた順序・内訳・外したworkerは、続けて行う配置の配置理由と一致し、配置が決定をしない状態では見積も決定を返さない。

queueに無い保存済みtask（実行中・終了済み・取消済み、依存するjobを待って保留されているCI task）の見積は、その入力を使う仮の追加実行を答える。配置はこれらのtaskを拾わないので、比べる相手の配置は無い。この場合はsourceがworkerでreadyかどうかで候補を絞らず、errorで答えたworkerを数値の無い候補として比べ、readyを確認できた転送だけを標本数0の「確認済みready」として0にする。

次の点は、queueにあるtaskでも配置と異なる。

- 古いcoordinator epochを理由にforecastを断られたとき、配置は自分を止める（fence）が、見積は止めず、答えの無いworkerとして外す。
- 能力が合わないworkerの `unmet` の文言は、見積の方が詳しいことがある（frozen toolchainの照合結果など）。外す結論は同じである。
- 未投入のcommandにはsourceも出力計画も無いので、上の入力の条件は使わず、転送・setup・回収はunknownのままである。

setupの見積は、実測の観測を数値へ変換するモデルが無いため、保存済みtaskでもunknownとする（cost modelが値を持つ場合を除く）。確認できた準備identityは前提として表示するだけで、setupの時間・readyの遅れ・admissionを変えない。

人向けの表示は次を含む。`--json` は同じ内容を `result` の `decision`（配置理由と同じ形）、`forecasts`、`components`（workerごとの転送・setup・回収の根拠）、`assumptions`、`online`、`conditional` で返す。

- 選んだworkerと、勝った理由（唯一の候補、最短、同程度の中で決めた規則）。選べないときはその旨。
- 比較に使った規則の数値（priority 1段の秒数、同程度の幅、forecastの有効時間）。
- 比べたworkerごとに、予測の種別（`measured` は時間、`fallback` は順位、数値が無ければ `unknown`）、使わなかったforecastの理由、priorityと補正量、親が置いているtask数、待ち時間、実行時間、条件付きかどうか。
- 転送・setup・回収のそれぞれについて、実測（標本数と残りの有効時間）、確認済みready、cost modelの値、仮の値（順位付け専用）、unknownとその理由のどれであるか。仮の値と `fallback` の順位は、時間として表示しない。
- 外したworkerと理由、満たしていない条件。
- workerごとに、他のメンバーの需要を含むかどうかとその件数（24.5）。
- この見積が前提にしていること（仮の追加実行であること、in-placeの内容は固定されないこと、など）。

## 必要なのは仕事ごとの予測

「このMacは平均何秒待つか」という値だけでは配置できない。対象の資源要求、先行キュー、準備が終わる時刻、後続step、並列上限によって変わるため、**このworkloadをこのworkerへ追加した場合の予測**を求める。

単一commandの概念式は次とする。

```text
開始まで = 準備完了まで + 準備完了時点からの資源待ち
完了まで = 開始まで + 実行時間 + 必須出力の回収時間
```

例えば、転送に10秒、現在のキューが30秒で解消し、実行に20秒かかるなら、回収を除いた完了予測は50秒である。10 + 30 + 20の60秒にはしない。転送中にも既存のキューが進む。

setupがCPUを使うcommandなら、単なる準備遅延へ隠さず資源を消費するnodeとして扱う。CI全体はDAGをsimulateし、並列stepの時間を単純合計しない。Unitの実測wall timeには内部の資源待ちが含まれるため、それを実行時間として使ってさらに待ち時間を足さない（local-ci v0.7.0以降は待ち時間を別に記録するので、待ちを除いた実行時間を使う。第23章）。

## WorkloadPlan

task-traffic-controller-coreへ渡す計画はCI非依存の構造とする。

| 要素 | 内容 |
| - | - |
| command node | command identity、予測weight／RSS／runtime、優先度 |
| dependency | 他node完了が開始条件となる関係 |
| external delay | 転送等、admission外の準備 |
| ready time | 現在から登録可能になるまでの相対時間 |
| concurrency | 実executorと同じ登録中・待機中・実行中の制約 |
| correlation | attempt、node、実ledger entryの対応 |
| shared dependency | 複数Unitが待つsetup builder等 |

未来のnodeはreadyになった時点で仮想キューへ入れる。全stepを最初からwaitingとして予約しない。ready順、FIFO、優先度、backfill、資源規則は実行時と一致させる。runtime条件やdynamic matrixで未確定の部分はunknownとして返す。

## 需要の二重計上と取りこぼしを防ぐ

forecastには、実ledger、workerが受理した未登録workload、親が送信中の未ACK workを含める。`attempt_id / node_id` を使い、実ledgerへ登録されたnodeは仮需要を置き換える。受理から登録への隙間で消えたり、実・仮の両方に数えたりしてはいけない。

workerはcoverageとsnapshot digestを返す。coverageは `attempt_id / plan_digest / expansion_seq / node_id` で範囲を特定し、dynamic matrixの追加nodeを、古いattempt単位の応答だけで考慮済みとしない。親は合成済みforecastに含まれるnodeをoverlayへ再追加しない。shared setup builderも1回だけ数える。

登録ACKだけで仮需要を消さず、今回のforecastに使う実ledger snapshotに同じnodeが存在するときだけ実需要へ置き換える。terminal確認済みnodeは除去する。別storeの読取り順序によって、実ledgerにもoverlayにも需要がないsnapshotを作らない。

workerは複数のメンバーから仕事を受ける。forecastは、他のメンバーが投げた仕事も、実ledgerにあるものとworkerが受理済みのものを需要に含める。coverageは聞いたメンバーの分だけを返し、他の分は件数だけを返す。他のメンバーが送信中でまだ届いていない仕事は見えないので、その分はsubmitの時点の断りで吸収する（24.5）。

forecastは読み取り専用とし、全workerへ100回照会してもqueue entry・lease・jobが増えない。snapshotは予約券ではなく、実行時は最新状態をlock下で再確認する。

## 不確実な予測

履歴なし、RSS不明、実行時間超過、live memory guard、古いsample、toolchain変更は予測品質へ反映する。未知を0秒・0byte・確実に空きと解釈しない。

APIは `measured / fallback / unknown` の出所、履歴件数、条件付き予測か、現在の待機理由を返す。運用開始直後も配置を止めないよう、設定されたfallbackと現在のqueue chargeによる順位付けを提供する。その内部用ranking値を、測定済みETAとして表示しない。ranking値は応答の `target.ranking`（`start_in_ms`・`finish_in_ms`）に入れる。runtimeが測定済みでないことだけが理由で時刻を返せないときに付け、それ以外では省く。

`runtime_source` は、対象planのnodeに加えて、対象より前に積んだ需要（登録前に受理したnodeなど、履歴runtimeを持たずfallbackで見積もるもの）のうち最も悪い出所を返す。`unknown` は、runtimeが確定していないnode（dynamic matrixなど）をworkerが受け取ったときの出所である。現在のwire（第11章）のnodeにはその印が無いので、workerは `unknown` を自分では作らない。履歴の無いnodeは `fallback`、実行時間を超えたentryは `overrun` と `null_reasons` で示す。受理済みattemptの内部依存は無視し、未完了のnodeをすべて前の需要に数える（実際より遅く見積もる側に倒す）。未来の外部CPU・メモリ負荷の解消時刻は保証しない。

見積のTTLは既定2秒（`scheduler.forecast_ttl_ms`）。workerは応答の `ttl_ms` にこの設定値を入れ、有効性（`validity`）の判定にも同じ値を使う。経過時間は呼び出し側が単調時計で測って渡す（壁時計しかないときは、時計が戻っても0に丸める）。親（scheduler）は、照会を送る直前から決定の時点までを単調時計で測り、workerの `ttl_ms` と親自身の `forecast_ttl_ms` の短い方で判定する。boot IDとagent epochは、同じtickの `inspect` の答えと比べる。使えない答えのworkerは `unknown` として扱う。期限切れ、worker boot／agent epoch／policy変更で見積を更新する。長い準備の終了後にも実際のadmissionを再確認する。

# 10. 永続状態と実行単位

## 主なidentity

| ID | 用途 |
| - | - |
| cluster_id / coordinator_id | `cluster_id` はグループのIDで、グループ鍵と組で信頼の境界を決める。`coordinator_id` はメンバーのcoordinatorの導入で、グループの中でrunを投げたメンバーを区別する。1つのメンバーは1つのグループに属する（第24章） |
| coordinator_epoch | 親の再起動後に古い制御を拒否する世代。投げたメンバー（`coordinator_id`）ごとに数え、workerはメンバーごとに最大値を持つ |
| worker_id / worker_boot_id | workerのidentityとOS bootの区別。`worker_id` はメンバーの名前で、グループの中で1つ |
| agent_epoch | agent再起動を区別し、古いforecastを失効 |
| run_id | 1回のCIまたはcommand投入。投げたメンバーのDBだけが正本 |
| task_id | 配置可能な論理的な仕事 |
| attempt_id / generation | 1回の実行試行と世代。workerは `(coordinator_id, attempt_id)` で持ち、投げたメンバーが違えば別のものとして扱う |
| node_id | setup・step等の実行node |
| request_id / payload_hash | 冪等な要求と同一内容の確認 |

親と各workerは、それぞれローカルdiskのSQLiteを正本とする。WALを使用し、受理・配置・terminal確定等の重要transactionは永続commit後に応答する。LAN共有directory上のSQLiteや、task-traffic-controller ledgerを分散DBとして使用しない。メンバーは親DB（自分が投げたrun）とworker DB（グループのメンバーから受けたattempt）の両方を持ち、runの記録を他のメンバーへ複製しない。

同じtaskに非terminalの実行attemptを複数持たない。新attemptは旧attemptのterminal／retiredが確認されてから作る。coordinator epochで古い制御要求を拒否するが、世代を増やしただけで古いプロセスが停止したとは扱わない。結果はattempt ID、generation、payload hashで採否を決める。再起動後にadoptした同じattemptの永続結果は、生成時のcoordinator epochが古くても回収する。

## 状態

```mermaid
stateDiagram-v2
    [*] --> Accepted
    Accepted --> Preparing
    Preparing --> Ready
    Ready --> Starting
    Starting --> Active
    Active --> Finalizing
    Finalizing --> Terminal
    Accepted --> Cancelling
    Preparing --> Cancelling
    Ready --> Cancelling
    Starting --> Cancelling
    Active --> Cancelling
    Cancelling --> Finalizing
    Terminal --> [*]
```

terminal outcomeは `succeeded / failed / cancelled / timed_out / interrupted` を区別する。各段階の準備失敗・起動失敗等もfinalizingを通して確定する。stepの `waiting_resources / running` はattemptの内側に置く。CIではactiveの途中にも資源待ちが起こる。

通信の観測状態 `known / reconciling / unknown` は実行状態と別に保持する。切断は失敗でも未実行でもない。workerで計算が終了しても、親が必須出力を取得中ならrunは `collecting` と表示する。

## 受理の段階

| receipt | 保証 |
| - | - |
| coordinator accepted | 親に入力snapshotと実行仕様が永続保存された |
| worker accepted | workerが同じattempt・同じ内容を永続保存した |
| command registered | supervisorがlocal leaseを保持し、実ledgerへの登録が確認できた |
| running | 対象commandの実行開始が確認できた |
| terminal | 終了内容と停止・cleanupの結果が永続確定した |
| collected | 必須結果・成果物が親で検証、永続保存された |

1秒以内に完了する仕事でも、registeredの観測を待ち損ねたことを理由にlost扱いしない。永続eventとterminalを照会できるようにする。

workerは `command registered` と `running` に初めて到達したとき、receiptの記録と同じtransactionでevent journalへ `attempt_receipt`（payload `receipt`）を1件書く。`watch_events` はこれを `receipt_reached` として返すので、receiptは到達順に、それぞれ1回だけ見える。leaseの失効も同じで、DBのleaseを失効にした最初の書き手（supervisorか、supervisorの起動前ならagent）が同じtransactionで `lease_expired` を1件書き、`watch_events` は `lease_expired` として返す。2回目以降の失効は何も書かない。supervisorが診断用に書く `supervisor_stop` の文言は、通信上のeventの判定に使わない。

# 11. 通信プロトコルと二重実行の防止

SSH上でversion付きの長さprefix JSON frameを使う。stdoutはprotocol専用、診断はstderrへ出す。大きなsource・artifactはchunk転送し、digestとoffsetで中断再開する。ログ転送がcontrol・heartbeatを塞がないよう、別streamまたは優先制御を実装する。

必要な操作は `hello`、`inspect`、`forecast`、`submit_attempt`、`get_attempt`、`list_attempts`、`renew_lease`、`cancel_attempt`、`retire_attempt`、`watch_events`、`read_log`、`get_result`、`ack_collected`、blobの転送・検証、secret bundleの転送（第22章）、環境差分の取得と導入（`env_report`・`env_sync`）、グループ鍵の確認（`group_proof`。24.2）、退役の通知（`identity_retired`。24.3）とする。

`env_sync` は、親が名指しした tool と厳密な版だけを worker 自身の registry（`toolchains.toml`）の inventory に照らして導入する。導入は待たずに `pending` を返し、親は同じ `sync_id`（導入1回ごとに親が決め、要求ごとの `request_id` とは別）で聞き直す。worker は `sync_id` ごとに導入への参照を1つ持ち、決着した結果は聞き直しに同じ答えで返す。同じ `sync_id` で別の tool を要求したら `CONFLICT`。1要求の tool は32個まで、同時に覚える `sync_id` は16個まで（超える要求は `OVERLOADED`）。古い epoch は `STALE_EPOCH`。停止中の worker は新しい `sync_id` を `CONFLICT`（停止中であることを述べる）で断り、既に覚えている `sync_id` には答え続ける。

worker 側の確認（registry・receipt・setup の照会）は、`sync_id` ごとに同時に1本だけ、要求とは別のスレッドで行う。確認を始めた要求は最長1.5秒だけそれを待ち、終わらなければ `pending` を返す。確認中に同じ `sync_id` で来た要求は、分かっている結果（未決着は `pending`）をその場で返し、確認を重ねない。遅い確認の結果は捨てず、次の要求に答える。60秒（`warmup_idle_ms`。設定 `coordinator.warmup_idle_seconds`、既定60、0は不可）聞かれない `sync_id` は参照を手放し、最後の参照なら導入を止める（確認中のものは手放さない）。tool を1つも含まない `env_sync` は、その `sync_id` の warm-up を今すぐ手放す要求である（親が待つのをやめたとき。確認中なら、その確認が戻ったときに手放す。知らない `sync_id` には何もせず空の結果を返す）。この要求を知らない古い worker は `CONFLICT` を返すので、親は答えを見ずに終わり、idle の時間に任せる。手放した後に同じ `sync_id` で聞かれたら、同じ内容の新しい導入として扱う。

親は `COMMUNICATION_UNKNOWN` を答えなしとして、`OVERLOADED` を「worker は居るが混んでいる」という答えとして、どちらも同じ `sync_id` で聞き直す。worker の答え（`pending`・`OVERLOADED`）の無い状態が2分続いたら打ち切る。一度 `pending` を見た後は、resident に繋がらないこと、resident が停止中と答えること、resident が worker をまだ掴んでいないこと（`NOT_FOUND`）も答えなしとして同じ2分の中で聞き直す（resident の再起動をまたぐ）。`pending` を見る前のそれらは、その場で止める。`pending` が続く間は最長1時間待つ（worker は準備の timeout 15分で `failed` にする）。`pending` を見た後に打ち切る・止めるときは、worker で導入が続いている可能性と、`workers diff` で確かめられることを伝える。

`hello` でprotocol versionとfeatureを決め、続く `group_proof` で、グループ（`cluster_id`）とグループ鍵を双方が確かめ合う。worker identity、profile、epoch、能力は、proofが合った後に `group_proof` の答えで返す（proofの前には返さない）。互換性のないworkerにはdispatchしない。proofが合うまで、workerは他の操作を受けず、接続した側は仕事もsecretも送らない。グループが違う接続、鍵が合わない接続、名前と `coordinator_id` の組が記録と違う接続、退役を告げられたidentityのままの接続は、理由を付けて断る（24.2・24.3・24.10）。鍵が更新されたら、workerは古い鍵で確かめた接続を閉じ、以後は新しい鍵のproofだけを受ける。断る理由は `PROTOCOL_INCOMPATIBLE` に付ける項目で、新しいerror codeは足さない。断られた側は、送り直しを繰り返さず、設定の不一致として `workers`・`group`・`doctor` に表示する。1つのworkerは複数のメンバーと同時に接続を持ち、attemptへの操作は、そのattemptをsubmitしたメンバーの分だけを対象にする（24.4）。

optional な feature `env_sync_v1` は、`env_sync` に答えられることを表す。双方が提供し、どちらも必須にしない（接続・通常の配置は片方が古くても動く）。合意していない接続へは親は `env_sync` を送らず `PROTOCOL_INCOMPATIBLE`（`workers upgrade` を案内）とし、worker も合意していない接続からの `env_sync` を断る。`env_sync` の `sync_id` と `pending` は、この feature を合意した peer にだけ送るので、schema version は上げない。

workerは受け取る量に上限を持ち、超える要求は一部だけ処理せずerrorで断る。以下の上限は、どれもそのworkerへ仕事を投げるすべてのメンバーの合計に対するものである（24.4）。

- `submit_attempt`: terminalでないattemptはworkerあたり32個まで（worker agentの設定 `max_live_attempts`）。超える新規submitは何も保存せず `OVERLOADED` で断り、親は後で同じattemptを送り直す。受理済みattemptの再送は上限に数えず、既存状態を返す。これとは別に、受付窓（worker自身の設定の `max_active_task + prefetch_per_worker`。第9章）を超える新規submitも、tombstoneを含めて何も保存せずに断り、`overloaded_reason.reason = "capacity"` を付ける（24.4）。`lease_ms` は24時間以下でworkerの監視間隔より長いこと、`deadline_ms`・各timeoutは7日以下であること。argv・env・cwdのNUL、空またはenv名に `=` を含むものは `INVALID_INPUT` とする。artifactの宣言（`artifacts`）は1024件まで、nameは255 byteまで、pathは1024 byteまでとし、超えるものと第13章の規則に反する宣言は `INVALID_INPUT` として何も保存しない。
- workspaceの準備は同時に4個まで（`max_concurrent_prepares`）。1つの準備は15分（fast-worktreeの既定timeoutと同じ）を超えたらattemptをfailedにする。
- `list_attempts`: `states` の絞り込みはDB側で行う。該当が500件を超えるときは先頭だけを返さず `INVALID_INPUT` とし、呼び出し側が `states` を絞る。
- `blob_put`（sourceの受け取り）: sourceは、manifest（そのsource自身のものが最初、続いてそれが名指しするsubmodule captureのもの）、content（manifestが名指しするファイル）、Git bundle（Git sourceに1つ）の3種類のblobで届く。必要なものが全部そろうまで、workerのobject storeにもcloneにも何も入れない。そろったあとも、sourceを断り得る検査（storeに既にあるファイルのサイズ、submodule captureの有無、bundleが展開できてmanifestの名指すcommitを運ぶこと）を全部先に済ませ、bundleはcloneの隣の作業用repository（cloneのrefを持ち、objectsは借りるだけ）で試す。断ったsource・途中で捨てたsourceは、storeにもcloneにも何も残さない。上限は次のとおりで、どれも書き込む前に、相手が申告したサイズで判定する。
  - 1つのmanifestは、workerの `max_manifest_bytes`（既定64 MiB）まで。送る側も同じ既定値（`BlobLimits::default()`）を守り、超えるmanifestはsourceの何も送る前に断る。
  - 1つのsourceが使う量は、manifestの申告サイズ、bundleの申告サイズ、manifestが名指しするファイルのサイズ（同じhashは1回）の合計で、32 GiBまで。超えるものは `INVALID_INPUT`。
  - 受け取り途中のsource全体では64 GiBまで。ディスクは、受け取り途中の全source（要求したsource自身を含む）が予約してまだ書いていない分と、今回増える分を引いたあとの空きが1 GiB以上。1つのsourceがディスクに予約する量は、置く分（上の合計）に、公開のときに一時的に重なる分（manifestをもう1回、bundleをもう1回、最大のファイルを2回）を足したもの。空きは、受け取り用ディレクトリ・store・clonesの3か所のうち最小の値を使う。超える要求は `OVERLOADED`（後で送り直せる）。受け取り途中のsourceは16個まで。この数と量の判定は、全source共通の1つのlockの下で行う。
  - 1つのsourceが持つbundleは1つ（別のbundleを送り始めたら前のものを捨てる）。そのsource自身のmanifestが置かれる前は、途中のmanifestも1つ。
  - 同じhashのファイルに、manifestによって違うサイズを付けることはできない（後から来たmanifestを `INVALID_INPUT` で断る）。storeに既にあるファイルに別のサイズを付けたmanifestも `INVALID_INPUT`（manifestが届いた時点で断る。その後にstoreへ入った場合は公開の直前に断り、そのsourceの受け取り済みのものを全部捨てる）。
  - storeに既にあるsubmodule captureは書き直さない。同じidで中身の違うmanifestは `INVALID_INPUT`。
  - bundleが前提にするcommitをworkerのcloneが持っていないときは、`CONFLICT`（`conflict_reason.reason = "bundle_base_missing"`、`have` 付き）で答える。そのbundleは保存せず、cloneも作らない。親は `have` を基にbundleを作り直して送る。bundleでないもの（headerが読めないもの）や、manifestが名指しするcommitを運ばないbundleは `INVALID_INPUT`（そのbundleを捨て、cloneには何も書かず、cloneが無ければ作らない）。`conflict_reason.reason` が知らない値のときは、理由なしの `CONFLICT` として扱う（`unknown` と読む）。`submit_attempt` が受理済みのattemptと異なるpayloadで届いたときの `CONFLICT` には `conflict_reason.reason = "payload_mismatch"` を付ける（送り直しても通らないことを示す）。
  - bundleは、dirty submoduleの中のdirty submoduleのcommitも、どの深さのものも運ぶ。workerは、そのsource自身のmanifestと、その下のdirty submoduleのmanifest全部（届いたもの、または以前に公開済みのもの。256個まで）をbundleと突き合わせる。各snapshot refがmanifestの名指すcommitであること、そのtreeがmanifestのtreeであること、どの階層のgitlinkも下の階層のcommitであることを確かめ、1つでも合わなければ `INVALID_INPUT` で断る。cloneに同じ名前のrefが別の値で既にあるときも `INVALID_INPUT` で、そのrefは書き換えない。refは全部入るか1つも入らないかのどちらかである（`git fetch --atomic`）。
  - `INVALID_INPUT` で断るのは、届いたもの（manifestとbundle）の検査が合わなかったときだけである。それ以外の失敗（Gitを起動できない、ディスクがいっぱい、refがlockされている、storeに書けない）は `INTERNAL` で答え、届いたbundleもほかの受け取り済みのものも残す。同じsourceの何かを送り直せば、続きから公開される。Gitは壊れたpackとディスク不足を同じ終了コードで返すので、headerは正しくpackだけが壊れたbundleも `INTERNAL` になる（エラーの文面では区別しない）。そのbundleは、別のbundleを送り始めるか、sourceを捨てるまで残る。作業用repositoryで展開できたbundleについて、cloneへの展開でGitが失敗しても、bundleのせいにはしない。
  - bundleをcloneへ展開したあとで断ることになったsource（cloneでcommitがsnapshot refの下に見つからない、公開の直前の検査が合わない）は、その展開が新しく作ったrefを消す（`git update-ref -d <ref> <commit>`。展開の前からあったrefと、公開済みになったsubmodule captureのsnapshot refは残す）。そのbundleのために作ったcloneは、refが1つも残らなければ消す。repositoryごとのlockは、公開するか断るまで持ち続ける。
  - workerは起動時に `git --version` を確かめ、Gitを起動できないとき、2.31より古いとき（`git fetch --atomic` が無い）は起動しない（`BLOCKED`、`environment_mismatch`）。
  - 1つのcloneへbundleを展開するのは同時に1つだけで、repositoryごとのlockで順番に行う（sourceのlockを取ったあとに取る）。待ち時間の上限を超えたら `OVERLOADED` で答え、届いたbundleは残す（送り直せば続きから公開される）。
- `blob_verify`: 問い合わせたblobごとの present / missing（受信済みoffsetつき）/ corrupt のほかに、sourceが公開済みか（`ready`）、bundleが要るか（`bundle_needed`）、sourceのrepositoryのcloneが持つcommit（`have`、64個まで。そのsourceのcommitで既にあるものが先、続いて、新しいcommitの上で作られたsnapshotの先端が先）を返す。何も変更しない。
- `blob_get`: attemptの保存済みresultに載っているartifactだけを返す。artifactのファイルは、途中のディレクトリを1つずつsymlinkをたどらずに開いて読む。
- `secret_bundle_put`: workspaceがまだ無いときは `NOT_FOUND`（送り直す）、commandが始まり得る状態以降は `CONFLICT`（確定）、cancel・retire済みは `RETIRED`。
- `OVERLOADED` は「今は受けられない。何も行っていないので後で同じ要求を送り直せる」を表す（live attemptの上限、DBのbusy、bulk streamの満杯）。CLIの終了コードは69（70〜79は他のcodeが使用済み）。

## 冪等性

同じ `attempt_id + payload_hash` のsubmit再送は、既存状態を返す。異なるpayloadならconflictとする。workerはacceptedをcommitする前にuser commandを起動しない。ACKを返す前後のcrashを想定し、親は同じattemptを同じworkerへ照会・再送する。

各command nodeには唯一のsupervisorによる実行claimと排他lockを持たせ、spawn前にlaunch intentを永続化する。setup builderにも同じ規則を適用する。starting状態からの復旧はprocess identityと照合し、起動の有無が不明なら再spawnしない。

**ACKが失われたことを理由に、別workerで同じ仕事を始めない。** 一時的な `not_found` だけでも再配置の証拠として不足する。遅れたsubmitが後から到着する可能性があるため、元workerにretire tombstoneを永続化し、未開始または停止済みを確認してから新attemptを作る。

元workerがofflineで確認できなければunknownのまま保持する。leaseが切れたはずだという時間推測だけで、別workerへ自動再投入しない。これにより、障害時に可用性より重複防止を優先する箇所を明確にする。

親（coordinator）は、attemptごとに「親DBに記録済みの事実」と「workerの今の答え」だけから次の1手を決める。親がどの時点で落ちても、次の起動が同じ規則で続きを行う。

- 答えが無い（切断・timeout）ときはunknownにして保持する。再送も再配置も失敗の確定もしない。
- retireの依頼があれば最初に送り、次にcancel、その後に未ACKのsubmitの再送を行う。再送は記録済みのpayloadを同じworkerへ送る場合に限り、payloadのhashが記録と違えば送らない。
- workerのACKを記録済みのattemptに `not_found` が返っても、再送せずunknownのまま保持する。
- retireの答えは、返ってきた語（`not_started`・`stopped`・`already_terminal`）ではなく、attemptがどう終わったかで分ける。`unconfirmed` は待つ。それ以外の答えのときは続けて `get_attempt` で終わり方を確かめる。`cancelled`・`interrupted`（完了せずに止まった）なら確認済みとし、taskを次のgenerationで配置できるようにする。`succeeded`・`failed`・`timed_out`（自分で終わった）ならその結果を採用して再配置しない。workerがtombstoneだけを持つ（attemptが届いていない）ときは、答えが `not_started` の場合に限り確認済みとする。終わり方の答えが無い、または答えが食い違うときは、unknownのまま保持して再配置しない。commandはworkerが状態を読んでから止めるまでの間に自分で終わることがあり（答えは `stopped`）、retireの最初の答えが失われて再送すると `already_terminal` が返る。どの場合も同じ結論になるようにこの区別を置く。
- retireが確認済みになったときの扱いは、確認を記録するtransactionの中で、その時点の記録から決める。taskにcancelが記録されていれば、taskを空けずに親がそのattemptを `cancelled` に確定する。workerがsubmitを確定的に断ったことによるretireなら `failed` に確定する。どちらでもなければattemptをretire済みにしてtaskを空ける。terminalのattemptをretire済みにはしない（結果が確定したtaskを再び配置しないため）。
- 届く前にcancelしたattemptは、workerにtombstoneが書かれたことを確かめてから、親が `cancelled` を確定する。
- workerがsubmitをerrorで断ったとき、確定的な拒否として扱うのは、attempt自体についての答えだけとする。`INVALID_INPUT` と、同じattemptを別のpayloadで受理済みであることを示す `CONFLICT`（`conflict_reason.reason = "payload_mismatch"`）である。このときは、そのworkerへretireを送ってtombstoneを残し、確認できたら親がそのattemptを `failed` に確定する。別workerへは自動で配置しない。
- 容量による断り（`OVERLOADED` で `overloaded_reason.reason = "capacity"`）は、そのworkerが他のメンバーの仕事を含めて満杯で、何も保存しなかったことを表す。そのattemptを1回だけ送ってこの答えを得たときは、retireを送らずにattemptを閉じ（理由 `capacity_refused` は親DBにだけ記録する）、同じtransactionでattemptのevent `submit_refused_capacity` を残して、次のgenerationで別のworkerへ配置する。2回以上送っていた、または答えの無い送信があったときは、遅れて届くcopyが残り得るので、retireを依頼し、workerが未開始を確認してからtaskを空ける。どちらの場合もtaskは失敗にしない。除外は一時的で、そのworkerが `inspect` で空きを答えれば候補に戻る（24.4）。
- `attempt_id` が使用中という断り（`OVERLOADED` で `overloaded_reason.reason = "attempt_id_in_use"`）は、そのworkerが同じ `attempt_id` を別のメンバーのattemptとして持っていて、何も保存しなかったことを表す（24.4）。taskについての答えではないので、taskを失敗にしない。容量による断りと同じ規則で、1回だけ送ってこの答えを得たときはretireを送らずにattemptを閉じ、送信の結果が不確かなときはretireを依頼してworkerの未開始の確認を待つ。どちらの場合も、同じattemptを送り直さず、新しい `attempt_id`（次のgeneration）で配置し直す。そのworkerを候補から外さない。親はattemptのevent `submit_refused_attempt_id` を残す。`in-place` のtaskは自動で配置し直さない（第6章）ので、この断りと容量による断りを理由の無い `OVERLOADED` と同じに扱い、同じattemptを同じworkerへ後で送り直す。
- それ以外のerror（理由の無い `OVERLOADED`、`INTERNAL`、`COMMUNICATION_UNKNOWN`、`NOT_IMPLEMENTED`、`NOT_FOUND`、理由が `payload_mismatch` でない `CONFLICT`、`blocked_reason` が `secret_unavailable` か無い `BLOCKED`、親の知らないcode）は、workerの一時的な不調か、そのattemptについての答えではないものとして扱う。taskを失敗にせず、同じattemptを同じworkerへ後で送り直す（何回でも）。断られた理由は、attemptのevent `submit_deferred` に、答え（error codeと、`BLOCKED` ではその `blocked_reason` の組）ごとに1回記録する（待っている理由を表示できるようにするため）。
- workerが自分の側の理由だと明示した答え、つまり `PROTOCOL_INCOMPATIBLE` と、`blocked_reason` が `environment_mismatch` か `no_compatible_worker` の `BLOCKED` は、そのworkerでは解消しない理由として扱う。同じattemptに同じ答え（codeと `blocked_reason` の組）が2回返ったら（1回目は `submit_deferred` に記録済み）、schedulerがそのattemptのretireを依頼し（理由 `worker_cannot_run`）、同じtransactionでattemptのevent `worker_excluded` にworker・code・`blocked_reason` を残す。別の理由の `BLOCKED` が先に返っていても、それは2回目に数えない。workerが未開始を確認した時点でtaskが空き、次のgenerationで配置される。そのtaskは、理由 `worker_cannot_run` のretireが確認済みのattemptのworkerへは配置しない（retireの理由だけで判断する。親DBの記録から判断するので、親の再起動後も同じ）。taskは失敗にしない。全workerが外れたtaskは未配置のまま待ち、理由は `task_waiting` に残る。`secret_unavailable` はどのworkerでも同じなので、移さずに同じworkerへ送り直す。
- 答えが届いたが読めなかったとき（schema versionが違う、別の操作への答えだった）は、workerのerrorではなく「答えが無い」として扱う。workerは依頼を実行したかもしれないので、拒否には数えず、unknownのまま保持する。
- 配置を記録した直後でも、そのattemptにcancelかretireの依頼が記録されていれば、submitを送らない（次の確認でcancel・retireを先に届ける）。
- 親は1回のtickで全workerを並行して確認する。1つのworkerに使う時間には上限（`worker_budget_ms`、既定15秒）を置き、上限までに答えの無かったattemptはunknownにして次のtickで確認し直す。そのworkerにはそのtickで新しい配置を送らない。応答の遅いworkerが1台あっても、他のworkerのleaseの更新は遅れない。tickの終わりに行う配置のsubmitにも同じ長さの上限を置くので、1回のtickは上限の約2倍以内に終わる。この2倍が `lease_ms` より十分短くなるように設定する。配置の決定（transaction）は並行させず、1つずつ行う。workerとの通信を待つ間、coordinatorのactorを占有しない。応答しないworkerが何台あっても、CLIの要求と他のworkerとのやり取りは待たされない（24.7）。
- payloadを組み立てられないtaskは、受付（enqueue）の時点で断る。受付を通らずに親DBにある組み立てられないtaskは、配置のたびに飛ばして他のtaskの配置を続け、理由をrunのevent `task_unplaceable` に記録する（1回の親processにつき1回）。
- `STALE_EPOCH` を受け取った親は、新しい親に交代したものとして、以後は配置も制御も行わない。
- 配置を記録した未ACKの需要は、workerの受理を記録したときのほか、attemptがterminalになったとき、retireが確認されたときにも解消する。

retire・重複排除の最小記録のうち、親の記録（`retire_tombstone`）は、runの刈り取り（25.5）でそのattemptの行と一緒に消す。workerの記録は、attemptを消した後は重複防止の記録が受け持ち、25.6の (c) の規則で消す（attemptが届かなかったtombstoneは、24.4の規則で消してよい）。大量のログ・成果物をGCしても、古いsubmitを新規実行する状態に戻さない。完全消去・再登録は新しいworker identityとして扱い、旧identityのunknown attemptを自動的に再投入しない。

# 12. lease、停止、再接続、再起動

## 生存確認と実行lease

既定値はheartbeat 5秒、接続stale判定15秒、実行lease 60秒、TERM後の停止猶予10秒とする。これらは設定可能とし、相互の矛盾を検査する。

親がworkerの状態を受け取り、workerは親から制御・lease更新を受ける。短い切断では有効leaseの範囲で実行を継続する。期限が切れた場合は新しいuser commandを開始せず、実行中commandと所有serviceをTERM、猶予後KILLで停止する。agentが落ちてもattempt supervisorがleaseを独立して監視する。

leaseとdeadlineは受理の時点から数える。workspaceの準備中にleaseが切れたattemptはuser commandを開始せずinterruptedにし、deadlineが尽きていればtimed_outにする。supervisorには残りのdeadlineと、leaseの残り（lease seed）を渡す。

lease seedは、agentがsupervisorを起動する直前に測った「受理または最後のrenewから数えたleaseが切れる時刻」（workerの単調時計の値）と、そのとき適用済みだったlease key（`coordinator_epoch`・`lease_seq`）の組である。supervisorは自分のlease clockをこの時刻から始めるので、起動前に与えられたleaseを、起動時点から数え直して延ばすことはない。agentとsupervisorは同じhost・同じbootの同じ時計を読む。規則は次のとおりである。

- seedの残りは、leaseの1期間（`lease_ms`）を上限にする。別のbootの時計の値や誤った値が渡っても、1期間より長いleaseにはならない。pollの遅れ（`poll_ms`）は、通常のgrantと同じく差し引く。
- supervisorの起動時にDBで適用済みのlease keyがseedのkeyと違えば、seedを測った後にrenewが届いている。このときseedは無効とし、そのrenewを起動時点から1期間として数える（短く数えて、有効なleaseを切らない）。
- seedはsupervisorを起動するたびに書き直す。seedの無いspecでは、supervisorは起動時点から1期間を数える。
- 起動後のrenewは従来どおり、supervisorが気付いた時点から1期間として数える。

renewはsequenceとrequest IDで冪等にし、同じrenewの再送で期限を再延長しない。失効・cancel・retireが確定したattemptは、遅延renewで実行許可を復活させない。共有setup builderには独立leaseを設け、少なくとも1つの有効なconsumer参照がある間だけ更新する。全参照の失効時はbuilder自身のsupervisorが停止する。

lease期限・timeout・queue ageはmonotonicな経過時間で管理し、OSのsuspend時間を含める単調時計（macOSとLinuxで別の実装を持つ。第15章）を使う。異なるホストの絶対時刻を直接比較しない。UTCは表示・診断用とする。通常のsleepはworkerのcapacityを止めるため、実行中のidle sleep抑止を既定で有効にする。lid closeや停電からの復帰は別途reconcileする。

lease期限切れはworker側の停止方針である。親に停止証拠が届くまではunknownであり、別hostで実行してよい証明にはならない。

## キャンセルとtimeout

取消要求を先に永続化し、新しいspawnを禁止する。実行中のcommand専用process groupと追跡した子孫・serviceを止め、停止とcleanupを確認してからterminalを確定する。PIDだけではなくboot IDとプロセス開始identityを確認する。

signalを送る前の確認は次の規則に従う（recovery・timeout・lease切れによる停止も同じ）。

- pidが今どのprocessのものかの読み取り結果は、**identity**（生きたprocessがあり開始identityが読めた）、**zombie**（終了したがまだ回収されていないprocessがそのpidを持ち、開始identityが読めた。pidはまだ空いていない）、**processなし**（OSが「そのprocessは無い」と確答した場合だけ）、**不明**（権限・資源不足・解釈できない内容など、それ以外のすべての失敗）に分ける。不明を「processなし」にも「同じprocess」にも寄せない。zombieも開始identityで誰のものかを判定する（Linuxは `/proc/<pid>/stat`、macOSは `sysctl(KERN_PROC_PID)`。macOSの `proc_pidinfo` はzombieを「processなし」と答えるので使わない）。
- 記録したidentityと一致する生きたprocessのgroupだけに、番号を根拠にsignalを送る。identityが違えば（zombieでも）pidの再利用であり、送らない。leaderを回収済みなのにそのpidをzombieが持っている場合も他人のものであり、送らない。
- leaderが「processなし」と確答された場合、または記録したidentityのzombieである場合は、group番号だけでは誰のgroupか分からない（groupが一度空になれば、そのpidを得た別のprocessが自分のgroupを作り、子孫を残して消えることがある）。このときはgroupにmemberが残っているかを調べ、残っていれば、**memberの1つがそのcommandのものだと確認できたときに限り**signalを送る。確認の根拠は、task-traffic-controllerがcommandの環境に入れる印（managed idとscope）を持つprocess、またはcommandの実行中に記録された子孫（pidと開始時刻）が、今そのgroupに属し、groupを読んだ後も同じ開始時刻であること。根拠が無ければ不明として扱い、signalを送らない。自分がsignalを送った直後に限り、根拠の無いmember（終了途中のもの）が消えるのを停止の確認時間の範囲で待つ。
- supervisorが実行中のcommandのidentityを記録するのは、同じ読み取りで親pidがsupervisor自身だったときだけにする。commandの回収は別threadが行うので、読み取りの時点でそのpidが別のprocessのものになっていることがあり、そのidentityを記録すると後で「同じprocess」と誤認する。
- **不明のprocessにはsignalを送らず、groupの有無も調べない**。停止を確定せず、消えたとも扱わない。実行中のsupervisorは次のpollで読み直し、それでも確定できなければattemptを未解決（unresolved）のまま残して、次回のsupervise（recovery）が同じ確認をやり直す。理由はevent（`supervisor_stop_identity_unknown`、`supervisor_recover_node` の `verdict: Unknown`、`supervisor_unresolved` の `identity_unknown`）に残す。
- 未解決の理由がidentity不明だけのとき、supervisorは専用のexit code（5）で終わる。agentはこれをsupervisorの失敗に数えず、attemptを諦めない（`max_spawn_failures` の対象外）。`retry_ms` から始めて倍々に、最大32倍まで間隔を延ばしながらsuperviseを起動し直し、確定できるまで続ける。最初の1回と、間隔が上限に達したときに、event `agent_identity_unknown` を1件ずつ残す。待つ間もcancelとretireを見ており、新しく要求されたら待ちを打ち切ってすぐに見直す。identity不明以外の理由が1つでも混ざる未解決は、従来どおりexit code 3で、失敗として数える。
- 既知の制約: 根拠を得られないmember（環境を消して起動され、実行中にも記録されなかった子孫。macOSでは、環境をOSが見せないsystem binaryで記録もされなかったもの）がgroupに残ると、そのattemptは未解決のまま再確認を続ける。残ったprocessが終わる（または利用者が確かめて止める）まで解消しない。Linuxで `/proc` が別のPID namespaceのものだったり `hidepid` で他のprocessを隠していたりすると、存在するprocessが「processなし」に見える。この構成は対象外とし、workerは自分のPID namespaceの `/proc` を読める環境で動かす。判定の読み取りからsignalまでの間にpidが再利用される、ごく短い時間の競合は残る（OSにpidではなくprocessを指定してsignalを送る手段が両対応では無い）。
- identityは判定のときと、signalを送る直前のたびに読み直す。group番号がleaderのpidと異なる記録は、leaderからgroupを判定できないので不明として扱う。

自然終了とcancelが競合した場合は、条件付きtransactionで一度だけterminalを確定する。cancel受理とcancel完了は別の応答とする。queued／preparingの仕事も遅れてspawnしないことを確認する。

通常の実行契約はforeground commandと、beltが所有を追跡できるserviceである。独立daemonとして脱離する処理はservice宣言とcleanup契約を必要とする。任意コードの外部サービスへの副作用を取り消せるとは約束しない。

command実行timeout、task全体deadline、資源待ちtimeoutを区別する。実行timeoutにqueue待ちを黙って含めない。全体deadlineは準備・待機・実行・回収のどこで超過したかを記録する。

command実行timeoutは、attemptの既定値（`command_timeout_ms`）と、commandごとの値（`timeout_ms`）を持てる。commandごとの値があるcommandは、その実行中だけ既定値の代わりにそれを使う（既定値より長くても短くてもよく、既定値が無くてもよい）。値の無いcommandは既定値に従う。資源待ちtimeoutとdeadlineはattemptのもので、commandごとには変わらない。0と7日を超える値は受理の時点で拒否する。

1つのattemptのcommandは1つずつ順に実行する。順序は `depends_on` で決まり、commandは自分が `depends_on` に挙げたすべてのcommandの後に走る。挙げる相手は一覧の前後どちらにあってもよい（後ろのcommandを挙げてもよい）。次に走れるcommandが複数あるときは、一覧で先にあるものを先に走らせるので、同じ一覧からは常に同じ順序になり、依存の無い一覧は書かれた順に走る。commandが成功しなかった時点で残りは開始しないので、依存先が失敗したcommandは走らない。attemptに無い名前を挙げた `depends_on` と、循環（自分自身を挙げるものを含む）は、受理の時点で `INVALID_INPUT` として拒否し、何も保存しない。

## 復旧動作

| 障害 | 動作 |
| - | - |
| CLI終了 | durable jobは継続。IDで再接続 |
| SSH切断 | worker spoolへ出力。lease範囲で継続し、再接続で追跡 |
| 親process再起動 | process lock取得、epoch更新、非terminalをreconcileして同じattemptをadopt |
| 投げたメンバーの停止 | 止まるのはそのメンバーが投げたrunだけ。そのattemptはworkerでleaseの範囲だけ続き、切れたら止まる。他のメンバーのrunは同じworkerの上で続く。別のメンバーは引き継がない（24.6） |
| メンバーのマシンの停止（両方の役割） | そのメンバーが投げたrunは上の行、そのマシンをworkerとして使っていた他のメンバーのattemptは「worker OS再起動」等の行で扱う（24.6） |
| グループ鍵の更新 | メンバー間の接続を閉じ、新しい鍵でつなぎ直す。attemptはleaseの範囲で続く。次の鍵を持つメンバーは断られた時点で自分で切り替え、持たないメンバーは `group send-key` で戻る（24.2） |
| メンバーの削除 | 外したメンバーが投げたattemptは、残るworkerがleaseの失効で止める。外したメンバーの上のattemptはunknownのまま保持し、leaseと停止猶予が過ぎた後の `workers forget` で確定する。未回収の結果は失われる。SSHのアクセスは鍵の更新では消えない（24.2） |
| 退役済みidentityのworkerが戻った | 親はattachせず、退役を通知する。workerはそのメンバーのattemptだけを止める（24.3） |
| workerが容量を理由にsubmitを断った | workerは何も保存しない。1回だけ送って断られたattemptはそのまま閉じ、送信の結果が不確かなattemptはretireを確認してから、別のworkerへ置き直す。taskは失敗にしない（24.4） |
| 更新（`belt upgrade`）の失敗 | 期限までにcommitされなければ、serviceの入口が親DBとworker DBを一緒に退避へ戻して元の版で続ける。試用中は配置も確定も行わず、他のメンバーの要求にも「後で送り直せ」とだけ答えているので、二重実行は起きない（24.8） |
| worker agent再起動 | 生存supervisorへ再接続。DBのactive表示だけでcommandを再起動しない |
| supervisor異常終了 | 子孫・serviceを調査、停止・確定。起動済み不明なら自動再実行しない。identityを読めないprocessと、leaderが消えたあと自分のものと確認できないgroupにはsignalを送らず、未解決のまま残して、確定できるまで間隔を延ばしながらrecoveryで再確認する |
| worker OS再起動 | boot ID変更を検出し、旧所有資源を確認・cleanupしてinterruptedへ |
| 結果転送の中断 | offset・digestから回収だけを再開 |
| 入力転送の中断 | 同じsource IDの不足blobだけ再送 |
| secret bundle消失 | 親がrun存続中に保持する同じbundleを再送（第22章） |
| 一時workerの到達不能 | attemptはunknownのまま保持し、時間の経過で再投入しない。利用者が `workers forget` で消滅を宣言したときだけinterruptedに確定（第15章） |

任意commandの命令途中から再開する機能ではない。OS rebootで失われた計算は、interrupted確定後に明示retryする。安全に再実行できる仕事はプロジェクトの明示設定によるretry policyを使えるが、実行状態unknownのattemptには適用しない。

分散matrix groupは、coordinatorがsubmit前にglobal permitを永続確保し、permit IDをattemptへ結び付ける。terminal、またはretireと停止確認が揃うまで解放せず、unknownも枠を保持する。parent不在時は新しいpermitを発行しない。permitはrunを投げたメンバーが管理し、他のメンバーのrunとは共有しない。発行済みpermitのattemptはlease規則に従う。global semaphore、fail-fast、joinをdurable coordinatorが管理し、再起動時に復元する。

# 13. ログ、結果、成果物

workerはstdout／stderrをdiskへspoolし、network速度でcommandの出力を直接停止させない。stream ID、byte offset、event sequenceで再開・重複排除する。任意bytesを扱い、UTF-8でない出力も元データを保存する。

local spoolの既定上限はtaskごと1 GiBとする。到達した場合もpipeをdrainし、保存しなかったbytes数と `log_truncated` を必ず記録する。必要なら上限を変更できる。disk不足や結果保存失敗を成功で隠さない。

上限はtaskのattemptで共有する。attemptは、spoolを開いた時点で他のattemptが保存済みのbytes数を上限から引いた残りを使う。回収が済みGCがlog参照を外したattemptのbytesは数えない。同じtaskの複数のattemptが同時に走る場合、互いに開いた時点の値しか見ないため、合計が上限を超えうる（超過は、同時に走るattempt 1つにつき最大で上限1つ分）。attemptは通常は順に走るので、これは既知の近似とする。

**既知の逸脱（stdoutとstderrの合流）**: 現在のsupervisorは、commandのstdoutとstderrを1本のpipeで受け、両方を `stdout` streamとして保存する。`stderr` streamは常に空である。task-traffic-controllerがstderrの出力先を別に受け取れるようになった時点（task-traffic-controller側の変更が先）でstreamを分ける。それまで、streamごとのoffsetと `log_truncated` は合流後の1本について成り立つ。

spoolに関してsupervisorがevent journalへ書くeventは次のとおりである。

| event | 意味 | payload |
|---|---|---|
| `spool_failed` | spoolへ出力を保存できなかった。attemptは成功にならない（`interrupted`、cancel済みなら `cancelled`） | `stage`（`open`・`pipe`・`write`・`finish`）、`error`。同じ `stage` は1回だけ書く |
| `workspace_warning` | workspaceの用意で出た警告。supervisorの開始時に書く | `index`（0始まりの位置、文字列）、`message`（maskしてから4096 bytesで切る）。最大64件で、超えた分は件数を伝える1行にまとめる |

`stage` が `open` または `finish` の `spool_failed` を持つattemptは、spoolを使わずに確定しており、resultを持たない。spoolのdirectoryはlogとして参照され、最後に公開された位置が保存されるので、GCが他のattemptと同じく回収する。それ以外でterminalなのにresultが無いattemptは、supervisorが確定の途中で落ちたものであり、次のsupervisorの起動または回復が、閉じたspoolの最終位置からresultを補う。`workspace_warning` は位置（`index`）で重複を除くので、mask後に同じ文面になる別の警告も1件ずつ残る。

artifactにはname、relative path、size、content digest、source ID、plan digest、attempt ID、profile fingerprintを記録する。親の受取後にhashを検証し、atomicに公開する。必須artifactが揃うまでrunをsuccessにしない。回収失敗で計算済みcommandをやり直さない。

workerでは、supervisorがattemptを確定する前にartifactを保存する。resultは一度保存したら変えず、workspaceはterminalになった時点で消えるためである。supervisorのspecはartifactの一覧（name、workspace内のsource path、欠落時の扱い）と、記録に使うsource ID・plan digest・profile fingerprint、1ファイルの上限（既定1 GiB）、workspaceに置いたsecret fileの識別子の一覧（`secret_files`。fileごとのdevice番号とinode番号で、agentがmaskする値を読んだのと同じopen済みのfileから取る。省略できず、secret fileが無いattemptでは空の一覧を書く）を持つ。手順は次のとおりである。

1. 開始したcommandがすべて終わり、停止を確認した後（未解決のまま残るattemptでは行わない）、一覧のファイルをworkspaceから `<attempt>/artifacts/<name>` へ写す。commandの成否は問わない（失敗したcommandのreportも残す）。commandを1つも開始しなかったattempt（開始前のcancelなど）では何も探さない。例外はCI taskで、後続taskが結果を待つため、stepを1つも開始しなくても（条件でskippedになったJobだけのtaskなど）結果のartifact `belt-ci-task-result.json` だけは残す。宣言済みのartifactとoutputの探索は、launchが1つも無ければ行わない。
2. 保存できたartifactの一覧を、event journalの `artifacts_stored`（attemptに1件）として確定する。
3. その後にterminationとresultを確定する。resultのartifactは、常に `artifacts_stored` の一覧である。同じentryは、宣言されたが保存できなかったartifactの一覧（`not_stored`）も持ち、resultの `artifacts_not_stored` はこの一覧である。確定の途中でsupervisorが落ちても、次に確定する者（workspaceが既に無くてもよい）が同じ一覧をresultに入れる。`artifacts_stored` が読めないときは「artifactなし」とは扱わず、確定を失敗させる。
4. `artifacts_stored` に載らなかったファイル（一覧の確定前に落ちたsupervisorが写したもの）は、どのresultにも参照にも入らないので、確定の時点で `<attempt>/artifacts` から消す。spoolを使わずに確定するattempt（resultを持たない）では、保存済みのartifactもすべて消す。

保存できなかったartifact（ファイルが無い、通常ファイルでない、symlinkを経由する、上限を超える、同名で内容が違う、attemptのsecret fileそのものである、secretの値を含む等）は一覧から外し、event `artifact_not_stored`（`name`、maskした理由 `why`。nameごとに1回）を書く。attemptのterminationは変えない。必須artifactの欠落でrunを失敗にするかどうかは、親が判定する。workerは判定の材料だけをresultに残す。保存できなかった宣言は、resultの `artifacts_not_stored` に `name`、宣言の `path`、`if_missing`（`required`／`optional`）、`reason` を持つ項目として載る。`reason` は、宣言のpathに何も無いとき（ファイル、または途中のdirectoryが無い）`missing`、それ以外（通常ファイルでない、symlinkを経由する、上限を超える、同名で内容が違う、secret fileそのものである、secretの値を含む、保存先に書けない）は `refused` である。`missing` は宣言のpath側に何も無いときだけで、保存先のdirectoryが無いといった保存側の失敗は `refused` である。全部保存できたattemptのresultは `artifacts_not_stored` を持たない（`get_result` の応答では空の一覧）。commandを1つも開始しなかったattemptは何も探さないので、`artifacts` も `artifacts_not_stored` も空である。`artifacts_not_stored` はresultの一部としてresult digestに含まれ、resultと同じく一度保存したら変えない。一覧は1024件までとし、nameが管理領域の相対pathとして使えないもの、nameどうしが衝突するもの、sourceがworkspaceの外を指すものは、commandを開始する前にspecごと拒否する。

**artifactの宣言の経路**: 親はattempt payloadの `artifacts` でartifactを宣言する。1件は `name`、`path`、`if_missing` を持つ。

| 欄 | 意味 |
|---|---|
| `name` | artifactの名前。worker上の保存先 `<attempt>/artifacts/<name>` の相対pathにもなる |
| `path` | commandがファイルを残す場所。workspace rootからの相対path（secret_filesのpathと同じ基準） |
| `if_missing` | ファイルが無かったときの扱い。`required` または `optional`。省略できない |

`artifacts` はpayloadの他の欄と同じく `payload_hash` に含まれる。宣言が1件も無いpayloadは `artifacts` を書かない（空の一覧も書かない）ので、そのcanonical JSONと `payload_hash` は、この欄が無かったときのものと同じである。同じattempt IDで宣言（name・path・`if_missing`・並び順のどれか）が違うsubmitは、別のpayloadとして `CONFLICT` になる。宣言が無いattemptでは、agentは一覧の無いspecを書く。

workerは受理の時点で宣言を検査し、次のものを `INVALID_INPUT` として何も保存しない。

- 件数が1024件を超える。
- `name` が空、255 byteを超える、または管理領域の相対pathとして使えない（絶対path、`.`・`..`・空の要素、制御文字、`\`、`.tmp` で始まる要素、17階層以上）。
- `path` が1024 byteを超える、絶対path、`..` の要素を含む、または空・`.`・空の要素・制御文字（NULを含む）を含む。
- `name` が重複する、または別のartifactの `name` の下にある（`a` と `a/b`）。
- `path` が重複する。
- `path` がそのattemptのsecret fileのどれかと同じである。secret fileは、attemptのsource manifestの `secret_files`（`[source] secret_files` の宣言と、`.worktreeinclude` に一致したignoredファイル。22.3）である。

`name` どうし、`path` どうし、`path` とsecret fileのpathは、NFCと小文字にそろえた形で比べる（大文字小文字を区別しないfile systemで同じファイルになるものは同じとみなす）。

secret fileとの照合には、attemptのsource manifestがworkerのstoreに要る。受理の時点でsourceがworkerに届いていれば受理の時点で照合する。届いていなければ、workspaceを作った後、supervisorのspecを書く前に同じ照合を行い、一致する宣言があればcommandを1つも開始せずattemptをfailedにする（event `agent_prepare_failed`、code `INVALID_INPUT`）。この時点でもsource manifestが読めないときは、照合できないまま開始せず、同じくfailedにする。このときのcodeは読めなかった理由による。sourceがworkerのstoreに無ければ `NOT_FOUND`、storeにあるmanifestがpayloadの `manifest_digest` と違えば `CONFLICT`、storeが読めなければ `INTERNAL` である。どの場合も、secret fileを指す宣言のcommandは開始しない。

受理を後の照合へ回すのは、sourceがまだworkerのstoreに無い場合だけである。受理の時点でsourceはあるがmanifestがpayloadの `manifest_digest` と違う（`CONFLICT`）、またはstoreが読めない（`INTERNAL`）ときは、submitをそのcodeで拒否し、何も保存しない。artifactを宣言しないpayloadでは、受理の時点でsource manifestを読まない。

**secretをartifactにしない保証の範囲**: workerが保証するのは次の3つで、これ以外は検出しない。

1. 宣言の `path` がsecret fileのpathと同じ（上の比べ方で）なら、受理または開始前に拒否する。早く分かりやすく断るための検査で、これだけでは足りない（NFCと小文字化は、file systemの大文字小文字の畳み込みと同じではない。`ſ` と `s`、`ß` と `ss` などは別の名前として通る）。
2. supervisorは、保存するために開いたfileそのもののdevice番号とinode番号を、specの `secret_files` と比べる。一致すれば保存しない（`refused`）。名前は問わないので、file systemが同じfileとして開く別の綴り、hard link、renameしたsecret fileはここで止まる。
3. supervisorは、写しながら内容を調べ、logでmaskする値（secret fileの `KEY=VALUE` の値のうち、maskできる長さがあり、ありふれた語でないもの）のどれかを含むartifactを保存しない（`refused`。一時fileも残さない）。値が読み取りの区切りをまたいでも見つける。

検出しないもの: maskする値を持たないsecret file（`KEY=VALUE` 形式でないもの、値が短いかありふれた語であるもの）の写し、変換した写し（encode、圧縮、分割、暗号化）、commandがsecret fileを同じ内容の別fileに置き換えた後の扱い（3の内容の検査だけが残る）。3の検査は、secretの値と同じbyte列をたまたま含むartifactも拒否する。event `artifact_not_stored` の `why` は、どの理由で保存しなかったかを言うが、値そのものは含まない。

artifactの記録（`get_result` の `artifacts` の1件）は、name、相対path、size、content digest、source ID、plan digest、attempt ID、profile fingerprintを持つ。相対pathは宣言の `path` ではなく、保存先（`<attempt>/artifacts` からの相対path）で、`name` と同じである。source IDとplan digestはpayloadのもの、profile fingerprintはworkerが `hello` で名乗るものと同じ値である。

親は検証と永続commit後に冪等な `ack_collected` を送る。workerはそのACKを受理してから回収待ちのGC保護を解除する。単なるdownload完了やSSH正常終了を保存確認には使わない。

**親の回収の手順**: 親は、terminalになったattemptごとに回収の記録を1件持ち、次の順に進める。どの段階で親が落ちても、接続が切れても、次の回収は親に永続保存済みの事実から続きを行い、commandは実行し直さない。

1. `get_result` でresultを読み、`<attempt>/result.json` へ書いて（一時ファイル → fsync → rename → directoryのfsync）から、`result_digest` とともに記録する。記録と、result・logのファイルへのGC参照は同じtransactionで確定する。resultのattempt・generation・terminationが親の記録と違うもの、artifactの名前が相対pathとして使えないもの、同じ名前を2回載せたもの、1024件を超えるもの、上限（既定1 GiB）を超えるartifactを載せたものは記録しない。
2. `read_log` でstdout・stderrを終端まで読む。終端は `read_log` の `eof`（spoolが閉じられ、その末尾まで読んだ）であり、`log_truncated` と保存されなかったbytes数はこの応答から取る。event journalの `log_truncated` はterminalの後に書かれるので、logの完了の判定に使わない。resultを持たないattempt（`result_digest` がterminationだけのresultのもの）はspoolが閉じられないことがあるので、読めた分を取り、不完全（`complete: false`）と記録する。
3. resultに載っていて親が宣言したartifactを `blob_get` で取得する。途中までのbytesは `<attempt>/tmp/<sha256>.part` に置き、chunkごとにfsyncする。再開はこのファイルの長さから行う。全bytesが揃ったらsizeとdigestを照合し、合えばfsyncしてから `<attempt>/artifacts/<sha256>` へrenameする（合わなければ捨てて最初から取り直す）。親の保存先はdigestであり、workerが決めた名前を親のpathに使わない（大文字小文字だけが違う名前、他の名前のdirectoryになる名前、長すぎる名前が、親のファイルシステムで衝突も失敗もしない）。どの名前がどのdigestかは、resultと受領の記録が持つ。公開済みで受領の記録がまだ無いファイル（renameの直後に親が落ちた場合）は、記録の前にdigestを確かめ直す。受領の記録とそのファイルへのGC参照は同じtransactionで確定する。親が宣言していないartifactは取得しない。親が送ったpayload（宣言の出所）を読めないattemptは、宣言なしとして扱わず、回収もACKもしない。
4. すべてが検証され永続保存された後に、記録した `result_digest` で `ack_collected` を送る。送る直前に、記録したものがdiskにあることを確かめ直す: `result.json` を記録から書き直し、各logと各artifactを読んで記録した長さとdigestに照合し、ファイルとその上の各directoryをfsyncする（新しく作ったdirectoryとファイルは、作った時点でも親directoryをfsyncする）。記録と合わないものは記録を取り消して回収し直し、それが済むまでACKを送らない。workerが別のresultを持っていれば（resultなしで確定したattemptに後からresultが保存された場合）`CONFLICT` で断るので、親はresultを読み直し、digestが違えば1から回収し直す。workerは、digestを照合した読み取りの後にresultが保存された場合も、ACKの記録と同じtransactionで断る。
5. workerがACKを受理した記録と、attemptの `collected` を、同じtransactionで確定する。応答を失ったACKは送り直す（workerは同じACKを何度でも受理する）。送り直したACKにworkerが `NOT_FOUND`・`RETIRED` で答えた場合（解放して削除した後）は、親にすべてが揃っているので回収を閉じる。resultを持たないattemptへのACKは、workerがそのattemptのsupervisorのlockを取れたときだけ記録する（supervisorが生きている間は後からresultが保存されうるので `OVERLOADED` で断り、親は待つ）。resultなしでACKされたattemptへのresultの保存は、workerが失敗として断る。

回収の失敗に数えるのは、問い合わせ直しても変わらないものだけである: 全bytesを受け取ったobjectのdigest・sizeの不一致、問い合わせと合わない・自己矛盾した応答（使えない名前を含む）、workerの `NOT_FOUND`・`RETIRED`（workerはACKを待つ期限 `ack_wait_ms` を過ぎたresultだけを削除する）。これが、間に検証済みの前進（artifactの公開）を挟まずに3回続いたら回収を打ち切り、`COLLECTION_FAILED` を確定する。それ以外（接続の切断、`OVERLOADED`、worker内部の失敗 `INTERNAL`、未知のerror code、親自身のdiskやDBの失敗）は数えず、間隔を延ばしながら（既定1秒から倍々、最大64秒。切断は待たない）何度でも待つ。前の親が残した `.part`（末尾が壊れているかもしれない）がdigestに合わないときは、失敗に数えずに捨てて最初から取り直す。1つのattemptの失敗は、同じworkerの他のattemptの回収を止めない。打ち切った回収ではACKを送らず、途中のファイル（`tmp/`）を消す。打ち切った回収は開き直せる（resultの読み直しから。親に残っているファイルは検証し直す）。（開き直しの保存処理は実装にあるが、それを呼ぶ経路は無い。打ち切りの時点でACKは送らず、別の操作 `release_attempt` を送り、workerはその場で写しを消す。打ち切った回収を開き直しても `NOT_FOUND` で再び打ち切られる。開き直す経路を足す変更が、workerに写しを残す上限つきの期間を定める。25.6。）workerが未ACKのresultを保持するのは `ack_wait_ms` の間なので、それを過ぎて開き直した回収は `NOT_FOUND` で再び打ち切られる。

**必須artifactと任意artifactの判定**: 親が `required` で宣言したartifactがresultの `artifacts` に無い場合（`artifacts_not_stored` の `missing`・`refused`、またはどちらの一覧にも無い）、そのattemptは `collected` にならず、taskは成功にならない（commandが成功していても「必須artifactの欠落」）。resultは変わらないので、`missing` も `refused` も取り直さない。存在するartifactとlogは回収し、ACKも送る（workerに残す理由が無い）。`optional` の欠落はtaskの結果を変えない。親がterminationを確定したattempt（workerが受理していない、またはretireを親が確定した）は、workerに問い合わせずに回収を閉じる。

attemptごとの保存領域を分け、古いattemptの結果は診断用に保持しても新attemptの結果を上書きしない。CIの全expected Unit、展開済みmatrix、必須stepがterminalか、明示されたskip理由を持つことを検査する。空の結果集合や欠落resultをsuccessと解釈しない。フィルタによって実行対象0件なら `no_work` とし、「テスト成功」と区別する。

runの判定は、taskごとの「現在のattempt（retireされていないもの）の終わり方」と「閉じた回収の結果」から決める。期待するtaskが0件なら `no_work`、期待より少なければ `missing`、終わっていないか回収が閉じていないtaskがあれば `collecting`、全taskが成功しかつ回収が完全なら `passed`、それ以外は `failed` である。`no_work`・`missing`・`collecting` は成功ではない。

matrixのoutputは既定で元のmatrix順に集約し、完了到着順によるlast-write-winsを禁止する。明示shardはby-shard namespaceまたは指定reducerを使用する。CIの判定には既存のcondition、continue-on-error、fail-fastの意味を保持する。

汎用の成果物宣言に加え、初回版は `actions/upload-artifact@v4` と `actions/download-artifact@v4` の同一run内の同じworkflow内の基本動作を実装する。name、path、複数file、missing-file policy、download先を対応表に明記する。cross-run、GitHub側artifact IDやAPI連携等の未対応optionは事前errorにする。現行のような単純skipを完全対応として表示しない。

| action / option | 分散 `ci` での動作 |
| --- | --- |
| upload の `name`・`path` | 名前の既定値は `artifact`。workspace 内の相対パス、ディレクトリ、複数行のパターンで複数ファイルを選択できる。 |
| upload の `if-no-files-found` | `error` は step 失敗。`warn`（既定）と `ignore` は成果物を公開せずに step を終える。 |
| upload の `overwrite` | 既定では同一 workflow の既存名を拒否する。`true` は新しい版を割り当てる。親は各ファイルのサイズと SHA-256 を検証・永続化してから応答する。 |
| download の `name`・`pattern`・`path` | 同一 run・workflow 内で公開された版を選び、指定先（既定は workspace）へ展開する。複数名は各名前の下へ展開し、`merge-multiple: true` では同じ先へ展開する。衝突する展開先は拒否する。 |
| 転送・保持 | 1 回の upload は最大 4,096 ファイル、合計 1 GiB。接続が切れた場合、親は永続化済みのオフセットから取得を再開する。 |
| 対象外の指定 | 外部 run・repository、GitHub の artifact ID・認証 token・API に依存する指定は、run を受理する前にエラーとする。 |

中間artifactはupload step時点でimmutableに固定し、親で検証・永続保存したreceiptを公開してからstepを完了する。download側はこのreceiptを待つ。最終run回収とは別の処理とし、後段がUnit terminalまでartifactを待つdeadlockを作らない。同名artifactのconflictもworkflow単位で判定し、別workflowが同名のartifactをuploadしてもconflictにならない（local-ci 0.8.0以降の実装と同じ）。別workflowのartifactはdownloadできない。同名artifactは同じworkflow内では既定でconflict、明示overwriteは新しいartifact versionとして記録する。matrixにはvariantを含む名前または明示reducerを必要とする。

`outputs` は明示directoryへ展開する。元worktreeへ戻す操作を用意する場合は、実行時sourceと現在内容の差分を確認し、競合を拒否する。入力、ログ、結果の既定保持はterminal後7日とし、実行中・回収待ち・参照中cacheはGCしない。削除前に未回収必須artifactを検査する。親が回収した結果（result・log・artifact）は、回収が閉じた（`collected`、または回収の打ち切り・必須artifactの欠落を確定した）attemptについて消す。保持期間は、terminalと回収を閉じた時点の遅い方から数える（workerが長く不在で、terminalから保持期間を過ぎて回収された結果を、回収の直後に消さない）。回収の判定の記録は、runの刈り取り（25.5）でrunと一緒に消す。runは、終わってから `retention_days` 日で、行・結果・入力ごと親から消え、その後の `show`・`logs`・`outputs`・`retry`・`cancel` は `NOT_FOUND` になる。workerの側の写しは、ACKか `release_attempt` の受理でその場で消える（25.6）。

secret bundleの内容はartifact・outputs・ログspoolのいずれにも含めない（第22章）。

# 14. 実行環境と認証情報

workerは専用または指定された非root userで実行する。各hostのHOME、TMPDIR、PATH、shellとprofileを使い、親の環境変数を丸ごと転送しない。通常envは、profileのbase、投入時にcaptureしたpass-through、project、workflow、job、前stepのGITHUB_ENV、現在stepのenvの順に合成し、後の値を優先する。pass-throughの非secret値はretry時にも取り直さない。portool等の自動defaultsはworkflowより前に入れる。

GITHUB_WORKSPACE、attempt identity、TASK_TRAFFIC_CONTROLLER_LEASE、内部socket・state path等の予約値はruntimeが最後に設定し、設定による上書きをerrorにする。PATHはprofileのbaseに正当なGITHUB_PATH更新を適用する。HOME／TMPDIRはprofileとworkspace方針が決め、親の同名envを暗黙転送しない。

新しいworker／top-level supervisorへ、親の `TASK_TRAFFIC_CONTROLLER_LEASE` は持ち込まない。各command supervisorが自分のleaseを取得し、そのcommandの子だけに継承する。既にadmission済みstepの内部で呼ばれる正当なnested task-traffic-controllerは、同じscopeのleaseを検証して継承し、二重課金を避ける。

secretはprojectが名前で参照する、worker側profileのsecret providerまたは明示転送で解決する。永続spec、CLI引数、source manifestに平文secretを埋め込まない。転送する場合は認証済みSSH経由で実行時に渡し、再起動後はproviderから再解決する。値はlogからmaskするが、任意commandが加工した値まで完全に秘匿できる保証はしない。

providerのversion／revisionを実行契約へ結び付ける。同じ参照を再解決できない場合は `blocked: secret_unavailable` とし、空値や新しい値で黙って再開しない。明示転送だけのsecretは再供給元を登録するか、必要時に再供給を待つ。secret更新を伴う再実行はexecution fingerprintを変えた新attemptとして記録する。

必要なsecretやtoolが利用できなければ配置前errorまたはblockedにし、そのstepを成功扱いでskipしない。GitHub status用credentialは親に置き、通常workerへ渡さない。`github.token` と他のaction互換性は対応表に含める。

repo lock、port、container slotは実行host内の資源として扱う。parentのport割当をそのまま子へ渡さない。複数workerが同一のremote Docker engine等を使う場合は同じresource domainのlockを共有する。初回版でそのdomainを管理できない構成はdoctorで不適合とし、同一engineを独立資源として二重計上しない。

同一OS userの互換task-traffic-controller CLIとbeltは同じledger・lock・leaseの資源scopeを使う。共存保証の対象は、本仕様のreaper修正を含む更新後のtask-traffic-controller runtimeとする。未修正v0.4.2 binaryの同時使用まで無条件には保証しない。複数OS userでそれぞれ全CPU budgetを持つ構成にはしない。複数userを運用する場合は管理者が合計予算を分割するか、単一実行userへ集約する。

同じledgerに加え、scopeのeffective policyも一致させる。budgetやguardをworkerだけの別configで上書きしない。共通configを正本とし、必要なpolicy変更はdrainまたは協調更新で行う。ここでいう共通configは、そのhostのメンバー自身の設定である。他のメンバーは、仕事を投げるときに相手のhostのpolicyを上書きしない（メンバーごとに設定が違ってよく、違いは `inspect` とforecastの答えに現れる。24.4）。doctorは旧CLIとworkerのstate path・policy digestを確認する。nested task-traffic-controllerにも同じscopeを明示し、HOME／XDG系の偶然の違いで別budgetを作らない。

# 15. 親からの導入と常駐方式（macOS・Linux・一時worker）

1つのhostには1つのresidentを置き、そのresidentがcoordinatorとworker agentの両方の役割を持つ（第24章）。`init` が登録するserviceも、`workers add --install` が相手に登録するserviceも、この1つのresidentである。既にbeltを持つhostへ `workers add` したときは、serviceもbinaryも足さず、グループのIDと鍵を渡すだけにする（24.2）。`workers add --install` で導入されただけのhostは、仕事を受ける役割だけを持つ。そのhostで `belt init` を実行すると、同じresidentにcoordinatorの役割が加わり（グループは今のものを使い、自分の `coordinator_id` を発行する）、そのhostから投入できるようになる。このときも2つ目のworkerは作らない。未完了attemptまたは未回収の終端結果があれば件数を示して拒否し、各メンバーから `workers stop` でdrainするか待ってから `init` を再実行するよう案内する。workerの切替lockの下で既存serviceを停止してinit residentのserviceへ置き換え、グループID・鍵・メンバー名・pinsを引き継いで旧plistとjournalを退役させるが、停止後の失敗時は変更を戻してworker serviceを復旧する。1つのhostに、別のstate directoryを持つ2つ目のworkerを常駐させない（試験ハーネスを除く）。

SSHで到達できる、起動中のMacへworkerを導入・起動できる。Remote Loginの有効化、正しいSSH認証、known_hostsの確認を利用する。SSH agent forwardingや親のprivate key複製は既定にしない。これはworker processの起動であり、電源OFFのMacをSSHだけで起動する機能ではない。

既定はユーザーのLaunchAgentとする。ログインsessionが存在する開発用Mac向けで、logout中・login前の常駐を保証しない。無人の専用Macには、明示選択の `service_mode = "system-user"` を提供し、LaunchDaemonの `UserName` で非rootの実行userを指定する。service登録に管理権限が必要でも、CI自体をrootで実行しない。

launchd配下のagentはforegroundで動作する。attempt supervisorをagentの一時的なSSH process groupへ従属させず、agent再起動が所有commandへどのように伝わるかを検証する。supervisor自体の自動再起動によってuser commandを再実行してはならない。

system-user modeではlogin shell、GUI Keychain、Docker Desktop等が利用できるとは限らない。doctorが実際のservice user・service環境でprofileを検査し、必要能力を持つworkerだけを選ぶ。専用Macのreboot後の自動agent復帰はこのmodeで受入検証する。

## Linuxの常駐

LinuxのworkerもSSHで導入・起動する。既定はsystemdのuser serviceとする。ログアウト後も動かすにはlingerの有効化が要るため、doctorが確認する（`loginctl show-user <uid> --property=Linger`。offは警告とし、直すcommand `loginctl enable-linger <uid>` を示す。答えが得られないときは未確認とし、合格にしない）。親（coordinator）の `init` もLinuxではsystemdのuser serviceを使う。無人の専用機には、macOSのsystem-user modeに対応する明示選択の `service_mode = "system-user"` を提供し、systemdのsystem serviceを非rootの `User=` で動かす。service登録に管理権限が必要でも、CI自体をrootで実行しない。systemdを使わない環境（コンテナ内等）は常駐workerの対象にせず、必要なら一時workerとして扱う。

## OS依存部分

次のOS依存の部分は、macOSとLinuxの実装を別々に持つ（platform module）。attempt supervisorとworker agentは両OSで同じ契約を守る。

| 部分 | 契約 |
| - | - |
| suspendを含む単調時計 | 第12章のlease・timeout・queue ageに使う。両OSでsuspend中の経過を含める |
| boot ID | worker OSの再起動を検出する（第12章） |
| process identity | pidの再利用を区別するため、開始時刻等と組にして記録する。読み取り結果はidentity／zombie／processなし（OSの確答だけ）／不明で、不明をどれにも寄せない |
| sleep抑止 | 実行中のidle sleepを抑止する。Linuxでは系統のsuspendも対象とする |

## 一時worker（Google Colab等）

Google Colabのように、常駐できず、利用者が起動した間だけ存在する環境は「一時worker」とする（2026-11-10の決定）。

- **起動**: 利用者がノートブックから `belt worker --ephemeral` を起動する。LaunchAgentやsystemdへ登録せず、親からの導入・更新（`workers add --install`、`upgrade`）の対象にもしない。バイナリの配置は利用者が行う。
- **接続**: 親は、利用者が用意したトンネルを指すSSH接続（ユーザー設定の `ephemeral = true` のworker）でつなぐ。独自の未認証ポートを開けない原則（第3章）は一時workerでも変えない。
- **起動し直し**: killや電源断でstate（`~/.belt`）が残った場合、次の `belt worker --ephemeral` が、誰も使っていないことを証明できたときだけ自分で消して、新しいidentityで起動する。証明できなければ、何も消さず、何にもsignalを送らずに1で終わり、成り立たなかった条件を示す。作成の途中でkillされた起動が残したもの（markerが無く、`ephemeral.lock` と `ephemeral.json.part` の2つ以内）も、次の起動が消す。stateの中に別のfilesystemがmountされているときは、その先と親を残して1で終わる。常駐の `belt worker`（`init` そのもの、`init` のresident、`worker install`・`worker start` を含む）は、一時workerのstate（`ephemeral.json` があるもの）では断り、何も書かない。常駐workerが導入されているhost（workerのserviceのjournalか、`init` のjournalがある）では、一時workerは起動しない。規則と証明は25.4の (d) に書く。
- **グループ**: 起動時にグループのIDと鍵を受け取るので、鍵を持つどのメンバーも使える。identityの退役と `workers forget` は、それを行ったメンバーの記録の中だけで働く（24.3）。鍵が更新されたら、新しい鍵で起動し直す。
- **identity**: 起動ごとに新しいworker identityを発行する。以前のidentityの再接続として扱わない。親は最後に見たidentityを記録する。同じ名前で別のidentityが接続を試みた場合、利用者が旧identityを `workers forget` するまで新しい接続を拒否する。以前の起動のattemptを新しい起動へ送らない。
- **到達不能**: 一時workerへ配置したattemptは、接続が切れてもunknownのまま保持する。時間の経過や推測で別workerへ再投入しない（第11章・第12章）。
- **`workers forget`**: 利用者が `belt workers forget <worker>` で消滅を宣言したときだけ、そのworkerのunknownのattemptをinterruptedに確定し、そのidentityを退役させる。退役したidentityはtombstoneを永続保持し（消す時点は未決。25.16）、遅れて戻った同じidentityは接続させない（親はsubmitもrenewも送らず、退役を通知する。workerはそのメンバーのattemptをすぐに止める。通知が届かなければ、worker側のattemptはleaseが切れて止まる。24.3）。確定したattemptの回収は断念として閉じる（消えたworkerからは回収できない）。確定後の再実行は、プロジェクトの明示retry policyまたは利用者の明示retryによる。forgetは、接続中のworkerや通常のworkerには使えない（拒否する）。forgetが退役させるのはidentityだけで、workerの登録は一覧に残る。登録を一覧から外すのは `group rotate-key --remove` である。

親が送るバイナリは、workerのOSとarchが親と同じときだけ親自身のものとする。違うときは `--binary <path>` で指定されたbuildを送り、指定が無ければ必要なtarget triple（例: `x86_64-unknown-linux-gnu`）を示して何も送らずに断る。releaseの成果物を自動で取得することはしない。送ったバイナリは、導入・切替の前にworker上で `--version` を実行させ、親と同じversionであることを確かめる。違う場合は送ったcopyを消して断り、導入済みのversionには触れない。

バイナリはversion別directoryへ配置し、実行中supervisorのファイルを上書きしない。（前の版のdirectoryは、今は消さない。第25章の段階4で、名指すものの無い版を消す。）更新はdrain、配置、互換検査、service切替、health check、resumeの順に行う。失敗時は元versionへ戻せる。古いattemptの結果とspoolを読み取れるmigrationを含める。更新の単位はマシン（1つのbinary、両方の役割）で、drainはworker全体に対して行う。自分のマシンは `belt upgrade` で更新し、commitまでの制限と、親DB・worker DBを一緒に戻すrollbackは24.8に従う。

# 16. task-traffic-controllerに必要な仕様変更

調査時点の基準実装は `task-traffic-controller v0.4.2`、commit `8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713` である（以下の「現状との差分」とsource linksはこの版を根拠とする）。lib化（第23章）後の基準は task-traffic-controller 0.5.0（crates.io公開済み、main `18b2d45`、タグ `v0.5.0`）とする。

## 現状との差分

| 現行の事実 | 実用版で必要な変更 | 0.5.0 / 0.9.0 での状態 |
| - | - | - |
| `status --json` は主にledgerのserialize | 既存出力を維持し、version付きのinspectとforecastを追加 | 満たし済み。`inspect`（`Inspection`、schema_version・digest付き）と `forecast` を提供。inspectに独立のhistory quality項目は無く、任意の残り |
| ETAは既にqueueにあるIDを対象とし、履歴なしに60秒を使い、live guardを予測しない | 未投入command／WorkloadPlanを予測し、fallback・unknown・条件付きETAを区別 | 満たし済み。`estimate::command_plan` と `forecast` が履歴なし・fallback・unknown・条件付きを区別する（`plan_command` に相当） |
| `submit` は5秒で登録確認できなくてもwarningとIDを返せる | durable accepted、registered、runningのreceiptを分離 | 満たし済み。`managed::register` の `RegisteredReceipt`（冪等）と `try_admit`・`run_command` で段階を分ける |
| liveな `TASK_TRAFFIC_CONTROLLER_LEASE` を継承するとnested実行のadmission・計測を省略 | Unitを外側の普通のleaseで包まず、step単位で取得 | ほぼ満たし済み。`managed::run_command` でstep単位に取得できる。ただし `exec::Opts` にenv mapとstdioが無く、T1で足す |
| runnerがprocess-globalなsignal処理を使う | workerの並列threadへ直結せず、専用supervisorへ分離 | 満たし済み。lib側にprocess-globalなsignal状態・`process::exit` は無い（`Context.supervisor` で注入） |
| jobはwaiter不在でorphan停止する経路を持つ | beltではdurable lifecycleとleaseを使い、SSH切断をwaiter消失にしない | 満たし済み。launch intentとdurable lifecycle（`OwnerGuard`）で、SSH切断をwaiter消失にしない |
| ledger entryの除去とjob終了記録が別処理 | durableなterminal記録を先に確定してから資源登録を解放 | 満たし済み。`managed::finish`・`resolve` が終了を確定してから解放する |
| owner lease消失でentryをreapでき、残存子孫を確認しない | managed commandは停止確認とruntime metadataを使ってreconcileしてから解放 | 満たし済み。`reap.rs` の `verdict` がmanaged規則（sidecar・boot ID・子孫確認、不明はKeep）を扱う |
| `lock::snapshot` はdead entryのreapとledger書込みを伴う | forecast用に書き込まない読み取り専用経路を分ける | 満たし済み。`lock::snapshot_read_only` が共有lockで判定し、reapも保存もしない |

0.5.0で残るのは次である（調査: X1）。**T1（必須）**: `exec::Opts` にenv map（`env_clear` 後に設定）とstdio（stdout・stderrの追記先log path、stdinはnull）を足す。既存CLIは `None` 既定で挙動を変えず、履歴keyにenvを混ぜない。**T1（任意）**: `legacy_clock`（旧entryのUTCから推定したageの明記）、inspectのhistory quality。左欄の記述と下のsource linksは調査時点（v0.4.2）の事実であり、調査根拠として残す。

根拠：現行の [status](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/cmd/status.rs)、[ETA](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/eta.rs)、[非同期job](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/cmd/job.rs)、[execとnested lease](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/cmd/exec.rs)、[runner](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/runner.rs)。

## 新しいcore API

| API | 入力・出力と保証 |
| - | - |
| `plan_command` | 明示PlanContextとCommandSpecからResourcePlanを作る |
| `inspect` | 設定、実ledger、live metrics、履歴quality、snapshot digestを返す |
| `forecast` | snapshot、WorkloadPlan、future demandからnodeと全体の予測を返す。副作用なし |
| `register` | stable command IDとspec digestを登録し、RegisteredReceiptとowner guardを返す |
| `try_admit` | lock下の最新状態で許可または構造化待機理由を返す |
| `run_command` | admission済みcommandを専用supervisorから実行し、構造化Outcomeを返す |
| `record_measurement` | measurement IDで重複を除いて履歴へ反映する |

coreにstate directory、config、history、Clock、計測provider、実行contextを明示的に渡す。並列処理のためにプロセス全体のcwdやenvを一時変更するAPIにはしない。libraryから `process::exit` せず、旧CLIのexit code・signal再送はadapterが再現する。

新しいCLI入口として `task-traffic-controller inspect --json`、`task-traffic-controller estimate --json -- <command>` を追加する。beltはCLIの表示を解析せずcore APIを利用する。

forecast responseの概念例：

```json
{
  "schema_version": 1,
  "worker_id": "mini-a",
  "snapshot_digest": "sha256:...",
  "policy_digest": "sha256:...",
  "ttl_ms": 2000,
  "coverage": [{
    "attempt_id": "attempt-previous",
    "plan_digest": "sha256:previous-plan",
    "expansion_seq": 1,
    "node_ids": ["setup", "test"]
  }],
  "target": {
    "plan_digest": "sha256:...",
    "ready_in_ms": 10000,
    "queue_wait_after_ready_ms": 20000,
    "execution_ms": 20000,
    "finish_in_ms": 50000,
    "confidence": "medium",
    "runtime_source": "measured",
    "conditional": true,
    "assumptions": ["no new external load"]
  }
}
```

この `finish_in_ms` はworker計算終了までの例で、親へのartifact転送はbeltが別に評価する。値が計算できない場合は `null` と理由を返す。既存running entryが履歴runtimeを超えた場合はremainingをunknown／overrunとし、0へ丸めて空き扱いしない。

## admissionとの一貫性

現在時点の判定は、実行とforecastで同じ `admit` 関数、ResourcePlan、Limits、実測charge、queue ageを使う。未来の実測負荷の投影は仮定として区別する。既存policyがlive guardを省略する条件も明示し、forecastだけに別の隠れたpolicyを作らない。

履歴identityは、論理repository、相対subdir、work key、実行profileを基礎にする。attempt IDや毎回のcommit IDだけをkeyにして学習を失わない。sourceの大きな変更、toolchain、cold／warm setupの区別はqualityまたはprofileへ反映する。性能の違う親と子のruntimeをそのまま同一視しない。

CPU weightやRSSはadmissionと予測のための値であり、OSの強制CPU上限・メモリ上限を実装したという意味にはしない。

## 登録と終了の順序

1. workerがattemptとstable command IDを永続化する。
2. 専用supervisorがlocal leaseを取得する。
3. ledger lock下で同じIDの登録を確認または作成する。
4. registered eventを永続化してからadmissionを進める。
5. launch intentを永続化し、子の起動identityと実行開始を追跡する。
6. 終了・停止・cleanupと必要なspool位置を記録し、terminalをcommitする。
7. その後でledger entryとleaseを解放し、計測を冪等に反映する。

OSのprocess spawnとDB commitを1つのatomic transactionにはできない。child実行前のhandshakeとlaunch intentを使い、crash後に起動の有無が不明なら再spawnせず調査・停止・interrupted確定へ進む。履歴書込み失敗だけで仕事を再実行しない。

supervisorがSIGKILLされた場合、flockの消失だけでbudgetを解放してはならない。managed entryのreaperはruntime metadata、boot ID、process identity、所有serviceを照合し、子孫の停止を確認するまでquarantine中の資源chargeを保持する。metadataが読めない場合も空き扱いしない。新coreを使うtask-traffic-controller CLIも同じreaper規則を使う。

## `belt exec` の直接実行と単独CLIとの共存

residentが動いていないときの `belt exec`（直接実行、第4章）は、組み込みのtask-traffic-controller coreを、単独の `task-traffic-controller exec` と同じ入口で使う。coordinatorもworker agentも要らない。

- **台帳・scope・policy**: 単独CLIと同じ規則でstate scopeとconfigを決め、同じ台帳（ledger）・lock・履歴を使う。belt用の別の台帳や別のscopeを作らない。同じhostで単独CLIと同時に動かしても、admissionの合計は1つの予算に収まる（A05と同じ考え方）。
- **待ちと表示**: 待ちの理由、進捗の表示、nestedなlease（`TASK_TRAFFIC_CONTROLLER_LEASE` の継承）の扱いは単独CLIの `exec` と同じとする。表示はstderrに出す。
- **終了と中断**: commandのexit codeをそのまま返し、signalで終わった場合は128にsignal番号を足した値を返す。Ctrl-Cはcommandのprocess groupへ送り、終了を確かめてから台帳のentryを外す。ただしbeltが端末のforeground process groupにいるとき（`tcgetpgrp(tty) == getpgrp()`）は、SIGINTを子へ転送しない。端末がforeground groupの全員へSIGINTを直接届けるので、転送すると子が2回受けて強制終了と見なし、後始末を飛ばすためである。`kill` で送られたSIGINTは、この場合でも転送する。
- **直接実行で使えないoption**: `--snapshot`・`--ref`・`--in-place`・`--profile`・`--requires` はrunを要するので、residentが動いていないときは拒否する。拒否の文言はそのoption名を挙げ、residentを起動するか、optionを外すよう案内する。
- **`--json`**: commandのstdoutをstderrへ流し、stdoutは最後のJSON文書のために空けておく。
- **nested**: `TASK_TRAFFIC_CONTROLLER_NESTED=inherit` のときはadmissionを取らず、親のleaseを引き継いで実行する。`admit` と `inherit` 以外の値は拒否する。
- **residentの有無の判定**: socketへの問い合わせに加え、state dirの `startup.lock` と `coordinator.lock` を、共有・非ブロッキングのflockで調べる。調べるためにfileを作らず、取った共有lockはすぐ外す。どちらかが他のprocessに保持されていれば、residentが起動中か動作中と見なし、commandは実行しない。launchdやsystemdが起動したresidentには、process開始からcoordinator storeを開いてlockを取るまでの短い隙間があり、その間に調べると停止中と判定される（既知の制限）。
- **`ProcessLock` の取得**: WouldBlockのときは10 ms間隔で最大10回試し、それでも取れなければ `Locked` で失敗する。
- **版の違い**: 単独CLIのtask-traffic-controllerと組み込みcoreの版が違っても、台帳の形式が互換なら共存する。組み込みcoreが読めない台帳の版（新しい単独CLIが書いたもの）は書き換えずに断り、`doctor` が理由を示す。下の「既存stateとの互換」の規則（未修正binaryは不適合）はここでも同じである。

residentが動いているときの `exec` は、自分のlocal workerでも他のworkerでも、attemptのsupervisorが同じ台帳・scopeでadmissionを取る（「登録と終了の順序」）。直接実行・単独CLIは呼び出したshellの環境から、residentは保持したserviceの環境から（22.2）scopeを決める。両者のscopeが同じなら、直接実行と、residentの中のattemptと、単独CLIの3つが同じhostで同時に動いても、予算は1つである。scopeが違えば予算は別になるので、`doctor` がtraffic scopeの比較でその差を示す。

## 既存stateとの互換

旧 `status --json` と既存CLIの契約を維持する。既存ledger v1へ、旧binaryの保存処理で消える重要な新fieldを混在させない。correlation、accepted、managed ownership、event、log offsetはbelt storeのsidecarとして管理し、更新後のreaperが参照する。

CLIの互換維持と、未修正binaryとの同時互換は分ける。v0.4.2のreaperはmanaged ownershipを理解しないため、共有scopeでは修正版への更新を必須とする。doctorとbootstrapは実行中process、解決されるtask-traffic-controller binary、scopeのruntime compatibilityを検査し、未対応ならdrain・更新の必要を表示してmanaged dispatchを開始しない。`doctor` の `traffic_processes` は、単独CLIのscopeの台帳にある、CLIが起動したjobを数える。belt自身が登録したentryと、beltのattempt内で `exec` を入れ子で呼んで登録されたentry（親をたどるとbeltのentryに行き着くもの）は数えない。ownerのleaseが生きているjobは `live`、生きていないentryは `stale` とする。`live` があれば、CLIが管理entryを知らない版のときは `fail`、そうでなければ `notice` とする。`stale` だけのときは `notice`、どちらも無ければ `ok` とする。単独CLIも台帳も無いhost、初期化済みで何も動いていないhostは `ok` で、exit 0である。台帳を読めないとき、workerのhelperが古くて報告しないときは `not_checked` とする。旧単独jobを動かしたままstateを破壊する移行は行わない。

snapshot digestはcanonical ledger・policy・参照履歴・overlayから計算し、後付けrevision fieldだけには依存しない。旧entryのUTCから推定したageは `legacy_clock` と明記する。旧task-traffic-controllerと同じuser・state scopeで同じlock／leaseを使い、別budgetによる過剰実行を避ける。

変更対象は `src/main.rs`、`cmd/exec.rs`、`cmd/status.rs`、`cmd/job.rs`、`admit.rs`、`eta.rs`、`runner.rs`、`ledger.rs`、`lock.rs`、`jobs.rs`、`key.rs`、`gitctx.rs`、history／paths周辺と、抽出した `lib.rs` である。

# 17. local-ciに必要な仕様変更

調査時点の基準実装は `local-ci v0.6.2`、commit `873b4a72cc1e4f30d1e538cbfabd6c821682d08f` である（以下の変更仕様とsource linksはこの版を根拠とする）。lib化（第23章）後の基準は local-ci 0.9.0（crates.io公開済み、main `8666da7`、タグ `v0.9.0`）とする。

| 現行構造 | 変更仕様 | 0.5.0 / 0.9.0 での状態 |
| - | - | - |
| CLI内でrevision・branch・filterを決めて実行 | plannerとexecutorをlibraryへ分離し、FrozenPlanをwire化 | 満たし済み。`run::build_frozen_plan`（`FrozenPlan` はschema_version 4・digest付き、canonical JSON） |
| needs等でUnitをまとめる | Job DAGとして依存辺を保持し、Job単位で独立配置。さらにrunner対応のテストExecutionTask／Shardを自動分割 | 一部。`frozen::derive_job_dag` と `ExecutionTask`・`ShardGroup` は有る。shardは常に元task 1件で、分割は **L2・L3** |
| 全体でsetupをhoistし、global判定で元stepをskip | 元stepsを保持し、ExecutionTask／ShardごとのSetupRecipeと成功receiptで置換 | 一部。`setup::derive_setup_recipe`・`PreparedReceipt` は有る。`SetupRecipe`・`PreparedReceipt` にschema_version・digestが無く、**L1** |
| shell stepをtask-traffic-controller経由で実行する一方、setupに別経路がある | 全重いstep・setupを同じcommand supervisorへ流す | 残り。stepはバイナリを子プロセスで起動しており、lib経由のregister・try_admit・run_commandを差し込む口（`CommandExecutor` 相当のtrait）が無い。**L1（最重要）** |
| 絶対pathのtools・worktree・node_modulesを使う | symbolic planとhost固有materializationを分離 | 一部。`FrozenPlan` は相対pathで、`Workspace` は別structに分離済み。`TaskResult`/`StepResult.log` と `RunResult.run_dir` はhost pathを含み、wireに載せるなら相対化が要る。**L1** |
| 既存のworktree dirty検出はuntrackedを含めない | 新snapshot APIで未追跡を含む入力を固定 | 満たし済み。`source::capture::capture_source`（`SourceManifest` はschema_version 2・digest付き）が未追跡を含めて固定する |
| cache keyにhost環境の十分な区別がない | OS・arch・toolchain・profile・source・planを明示する | X1では未確認。実装時にS17で確認する |
| resultの集合から状態を決める | expected結果との完全性を検証し、欠落・no_workを区別 | 一部。`complete::validate_run_result` は有る。`RunResult` が `Serialize` のみで `Deserialize` が無い。**L1** |
| artifact action等をskipする経路がある | 必須のartifact基本動作を実装し、未対応は対応表とerrorへ | 満たし済み（local-ci 0.8.0以降）。`actions/upload-artifact@v4`・`download-artifact@v4` をworkflow単位のreceiptで実装済み |
| kill追跡（CANCEL・PGIDS・CONTAINERS・COMPOSE_PROJECTS）とworkflows dir既定値がprocess-global | attempt所有の状態として引数で渡す | 満たし済み。`CancelHandle`（`ExecContext.cancel`）がattempt所有で、process-globalな静的状態は無い |
| cache dir・tool探索・PATHがenvを直接読む | 実行contextのenv mapから読む | 満たし済み。`ExecContext::new(BTreeMap)` が環境を保持し、lib内に `std::env::var` は無い |

公開API（`capture_source`、`build_frozen_plan`、`derive_job_dag`、`derive_setup_recipe`、`execute_task`）は0.9.0に有る。`validate_run_result` は一部。`partition_tests` と `to_workload_plan` は無い。0.9.0で残るのは次である（調査: X1）。**L1**: step実行の差し込み口（trait。local-ciはtask-traffic-controllerのcrateに依存せず、belt側が実装してtask-traffic-controllerのlibを呼ぶ）、task demandの列挙（`to_workload_plan` 相当。TTCの型に依存しないDTOで、条件式・動的matrixは `conditional`）、各DTOのschema_version・digest、wireからhost pathを除くこと、`RunResult` の `Deserialize`。**L2・L3**: test sharding（`partition_tests` とrunner adapter。L2は土台とcargo test・Vitest・Jest、L3はPlaywright・Cypress・coverage merge・分割禁止条件）。左欄の記述と下のsource linksは調査時点（v0.6.2）の事実であり、調査根拠として残す。

根拠：[CLI・revision・status](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/main.rs)、[Unitとsetup](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/plan.rs)、[executor](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/exec.rs)、[Git処理](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/git.rs)、[cache](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/cache.rs)、[結果判定](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/report.rs)。

公開APIは少なくとも `capture_source`、`build_frozen_plan`、`derive_job_dag`、`partition_tests`、`derive_setup_recipe`、`to_workload_plan`、`execute_task`、`validate_run_result` とする。Plan、SourceManifest、Job、ExecutionTask、Shard、Step、SetupRecipe、Resultにはschema versionとdigestを付ける。worker側のhost pathをDTOへ埋め込まない。

`exec.rs` のprocess-globalなkill追跡をworker全体で共有せず、attempt所有のcommand・serviceとして管理する。step内の `GITHUB_OUTPUT`、`GITHUB_ENV`、`GITHUB_PATH` の意味を保持し、再接続で重複適用しない。port・container・repo lockをcanonicalなworker scopeで扱う。

変更対象は `main.rs`、`run.rs`、`plan.rs`、`workflow.rs`、`exec.rs`、`git.rs`、`cache.rs`、`report.rs`、`config.rs`、必要に応じて `expr.rs`、`matrix.rs`、`portool.rs` と、新しいlibrary／DTO／source capture moduleである。

## GitHub status

GitHub statusを投稿するのは親だけとし、互換性のため既定contextを `local-ci` とする。全expected結果と必須artifactの回収が完了してから最終状態を決める。子がUnit単位で同じcontextを書き換えてはいけない。

GitHub successは、公開commitと実際の検査入力の一致を確認し、全expected結果と必須artifactが揃ったrunに限る。dirty／synthetic／directory／in-placeの結果を元HEADのsuccessへ変換しない。tracked fileのexcludeや追加inputがあればcommit modeという名前だけで一致と判断しない。`local-ci status` 側にも同じ検証を適用する。secret_filesの内容は公開commitに含まれないため、それ自体はcommitとの一致判定を妨げない（ignoredファイルは元々commitの内容ではない）。

runの結果にはsource digestと元HEADを併記する。入力が一致する公開commitなら通常のstatusを使用できる。未push commit等へのstatus投稿不能はCI計算結果と分けて報告する。

実装（S19）は次のとおりとする。

- 投稿するのは親の `belt ci` のprocessだけで、waitが終わり、runの結果と親が回収した結果から計算結果を決めた後に1回だけ行う。再試行はしない。`--no-status` で投稿せず、`--status-context <name>` でcontextを変える。
- 計算結果は `success`／`failure`／`error`／`cancelled`／`pending` のいずれかとする。`success` は、runがsettleし、cancelされず、回収が全て閉じ、終了コードが0で、提出したplanの全taskに結果の行があり、全ての行が `success` か `skipped` のときに限る。runの結果（`ci` の終了コード、`wait`、`show` が読むもの）を判定し直さない: Jobが失敗したrun（commandが0以外のcodeで終わった、testが失敗した、Shardの集約がfailure）は `failure`、Jobの失敗ではないもの（timeout、signal、失われたattempt、回収の失敗など）は `error` とする（local-ciのstatusの対応と同じ）。fail-fastが残りのShardを止めただけのrunを `cancelled` にしない。Shardを持たないrunでも同じで、Jobが先に失敗した後にrun全体をcancelしても、結果は `failure` のままにする（先に起きた失敗を `error` や `cancelled` に書き換えない）。cancelで終わったrunは `cancelled` とする。`cancelled` と `pending` は最終状態ではないため投稿しない。descriptionの件数（`pass`・`fail`・`error`・`skip`）は、分割したJobを1件として数える（Jobの行を数え、そのShardの行は数えない。Jobの行がまだ無いShardだけ、その行を数える）。
- 宛先は入力をcaptureしたcommit（manifestの `original_head`）だけとし、そのcommitのtreeと検査した入力のtree（`worktree_tree`）が一致するときに限る。一致しないdirty worktree、未追跡fileを含むsnapshot、directory、dirtyなsubmoduleは、`success` に限らずどのstateも投稿しない。
- 投稿は `gh api repos/<owner>/<name>/statuses/<sha>` に `state`・`context`・`description` を渡す。credentialは親の `gh` のものだけを使い、task、plan、residentへの要求に含めない。`gh` はPATHの絶対pathのentryからだけ探し、repositoryの外をcwdにして実行し、30秒で打ち切る（`failed`、`timed out`）。`gh` は自分専用のprocess groupで動かし、時間切れでは `gh` が起こしたprocessごとgroup全体をSIGKILLで止める。stdoutとstderrは動いている間に読み続け、pipeの容量を超える出力でも止めない。
- 投稿先のrepositoryは、GitHubのhost（`github.com`、`GH_HOST` があればそのhost）にあるoriginで、入力をcaptureしたときのrepository（manifestの `repository_id`）と同じものに限る。違えば投稿しない（`skipped`）。
- 結果は計算結果（`computed`）と投稿（`posting`: `posted`／`skipped`／`failed`）に分け、`source_digest` と `original_head` を併記する。`ci` の出力（textの `status:` 行、`--json` の `result.github_status`）と `show <run>` の `run.github_status` に出す。`failed` と `skipped` は `ci` の終了コードとrunの状態を変えない。
- 後から投稿し直すcommandは `belt ci status <run> [--post] [--status-context <name>]` とする。`--post` なしは表示だけで、何も投稿せず、何も記録しない。run に最後に記録された投稿（なければ `none recorded`）と、今の状態で投稿するもの（`would post: <context>=<state> to <sha> (<description>)`）、または投稿しない理由（`would not post: <理由>`）を出す。`--post` は、親が記録したrunの結果から、`ci` の終了時と同じ関数で状態を決め直し、GitHubのcommit statusとして投稿する。投稿できなかった場合（`failed`・`skipped`）は終了コード1で終わる。`--json` は `result` に同じ内容を入れる。
  - 拒否する場合: `ci` のrunでない（`NOT_FOUND`）、まだ終わっていない、cancelされた、結果の回収が閉じていない、結果を読めない、`--snapshot`・`--ref`・`--no-status` を併用した。拒否では何も投稿せず、何も記録しない。
  - contextは、`--status-context` があればそれ、なければ最後に投稿した（失敗を含む）ときのcontext、それもなければ `local-ci` とする。投稿を `skipped` で終えた記録はcontextを持たないので、決め手にならない。
  - 記録: 投稿した結果と、投稿に失敗した結果は、runの記録（`github_status`）に残る。この呼び出し自体が原因で投稿しなかった場合（例: 入力を読めない、source storeが使用中）は、GitHubに何が載っているかを表さず、最後の本物の試みを隠すので、runの記録に残さず、表示だけにする。記録できなかったときは、stderrに警告を出し、結果の `recorded`・`record_error` に示す。

## workflow互換性

既存のlocal-ci対応範囲を出発点とし、必要なworkflowをplan時に診断する。未対応action・expression・optionを黙ってsuccessへ変換しない。必要な機能は実装するか、具体的な理由を示すerrorにする。既存利用者が許容していたskipは互換modeで明示できるが、そのrunは `incomplete` とし、厳密なCI passやGitHub successにはしない。

公開する対応表には、checkout、tool setup、shell、needs、matrix、条件式、services、secrets、artifact、cacheの対応範囲と制約を列挙する。任意のactionへの完全互換は主張しない。実運用対象のworkflowが通常modeで最後まで動作することを、release gateとする。

# 18. belt側の新規実装と互換移行

| module | 実装内容 |
| - | - |
| cli | 単一command、JSON、進捗、再接続 |
| coordinator | durable queue、scheduler、matrix group、結果集約 |
| protocol | version、request identity、framing、blob・log resume |
| worker | profile検査、受理、future demand、supervisor管理 |
| supervisor | lease、step subprocess、signal、process identity、cleanup |
| source | Git／worktree／directory capture、object転送、manifest |
| secrets | secret bundleのcapture・転送・配置・消去（第22章） |
| toolchain | 版指定の解決、版管理ツールadapter、導入、環境差分（第22章） |
| workspace | setup singleflight、template、private workspace、local locks、組み込みのfast-worktree・portoolの呼び出し（22.5） |
| results | 完全性検査、artifact、spool、retention |
| service | launchd、SSH bootstrap、更新・rollback、doctor |
| storage | schema migration、transaction、tombstone、event journal |
| group | グループ鍵の確認・配布・更新、投げたメンバーごとのepochとattemptの所有、容量による断り、退役の通知（第24章） |

既存のtask-traffic-controllerを利用中なら、そのstate scopeを検出し、旧jobをdrainして必要なruntime更新を行ったうえで同じbudgetを使う。別scopeへ移す必要がある場合も移行を明示する。履歴が取り込めなくてもcold startとして動作させ、履歴欠落を資源不足や0秒予測に変換しない。

既存 `.local-ci.toml` とworkflowは読み込み、`belt ci plan` で互換性・分散単位・必要profileを表示する。1台だけでも全く同じcoordinator／worker経路を通し、子が増えたときだけ別実装へ切り替わる構造にしない。例外は、residentが動いていないときの `exec` の直接実行（第4章・第16章）だけで、これはrunを作らず、単独の `task-traffic-controller exec` と同じ働きをする。1台だけのグループと、複数のメンバーがそれぞれ投入するグループも、同じ経路である。

新しい operation・enum 値・必須フィールドは、hello の optional feature に紐づけ、合意した peer にだけ送る（N と N+1 が同じ cluster に居ても、接続と配置が動くこと）。error code を増やすときも同じ扱いとし、合意していない peer には読めない値を返さない。

protocol／storage migrationは互換表を持ち、未知versionをその場で書き換えない。更新前にjob状態を保護し、稼働中attemptを旧versionのまま追跡できる。親DBのcopyを別Macで同時起動してactive-activeにする操作はサポートしない。

版の間の互換はN／N+1とする。新しいfeatureは、まず「提供するが要求しない」版をすべてのpeer（親のresidentとworker）へ配り、要求（required）へ変えるのはその次の版からにする。第24章のfeatureは最初の公開版から必須とする（24.10）。`workers upgrade` は、動いているresidentが親のbinaryと同じbuildのときだけ進め、healthの `hello` はresidentが要求するfeatureで行う。rollbackで戻した版は、その版がupgrade前に合意していたfeatureだけで検査する。

workerの `upgrade` をrollbackできるのは、親が新しい版のhealthを確かめてcommitするまでである。その間workerはdrainのままで、切替前に退避したworker DBをrollbackで戻す。したがって試用中の版が書いた記録（ackやGCの進み）はrollbackで切替前へ巻き戻る。commitの後は戻せない。

# 19. 受入条件

以下をすべて通した時点を初回実用版の完成とする。単体テストだけでなく、実際の2台以上のMac、既存ツールとの共存、障害注入を含める。

| ID | シナリオ | 合格条件 |
| - | - | - |
| A01 | 親と2子へ同時投入 | 発注側がhostを選ばず、親と子の両方で実行される |
| A02 | 途中からworker追加 | 親の未配置backlogを新workerが実際に引き取る |
| A03 | busy workerとcold worker | queue、setup、転送を含む判断理由が説明できる |
| A04 | 複数CLIの同時submit | 未ACK需要を含め、競合する配置commitを直列化する |
| A05 | 互換task-traffic-controller CLIとの同時利用 | 更新後coreで同じbudget・policyを共有。未修正binaryをdoctorが不適合とする |
| A06 | estimateを100回実行 | ledger entry、lease、job、受理予約を増やさない |
| A07 | ready遅延10秒、queue30秒、runtime20秒 | 回収を除く単一commandのfinishを50秒と計算する |
| A08 | acceptedからregisteredへの遷移 | future demandの欠落・二重計上がない |
| A09 | 履歴なし・overrun・memory block | unknown／条件付き／fallbackの区別を保持する |
| A10 | 複数step・setup・matrixのforecast | 最初のstepだけをUnit全体のETAとして返さない |
| A11 | staged／unstaged／untracked／削除／mode／binary／symlink | 同じ入力を実行し、元index・refs・worktreeを変更しない |
| A12 | capture中のsave・rename・HEAD変更 | 再試行し、確定不能ならSOURCE_BUSYでdispatchしない |
| A13 | source公開・転送中のcrash | 不完全snapshotを実行せず、同じsource IDで再開する |
| A14 | 未push commit、submodule、LFS | 必要objects・payloadを固定して実行する |
| A15 | global setupの代表jobが別worker | consumer側の必要installが欠落しない |
| A16 | 同じscriptでshell／job envが異なる | 誤ったtemplateを再利用しない |
| A17 | template A使用中にA+Bを構築 | Aを変更しない。1 consumerのcancelが他を壊さない |
| A18 | 同じattemptを100回再送 | user commandは最大1回だけ開始される |
| A19 | accepted ACK消失・遅延submit | 別workerで重複せず、retire後の古いsubmitは拒否される |
| A20 | spawn前後・terminal前後のcrash | 起動不明を自動再実行せず、結果を一度だけ確定。残存子孫の停止前にbudgetを解放しない |
| A21 | CLI終了・SSH切断30秒超 | 旧orphan watchdogで止めず、lease内は継続して再接続できる |
| A22 | lease失効・親長時間停止 | workerが停止を実行。親は停止確認までunknownを保持する |
| A23 | 親／agent再起動 | supervisorをadoptし、同じ仕事を再spawnしない |
| A24 | worker reboot | boot IDを検知、cleanup、interrupted確定後にretryできる |
| A25 | 子孫command・serviceのcancel | TERM／KILLと停止確認を行い、自然終了との競合でもterminalは1つ |
| A26 | wall clock変更・Mac sleep | lease／timeoutが逆行しない。復帰後に整合を確認する |
| A27 | 分散matrix | group全体のmax-parallel、fail-fast、join、決定的output集約が成立 |
| A28 | shared workspace依存のUnit | 任意に分割せず、planに理由を表示する |
| A29 | 大量log・切断・非UTF-8 | offsetで再開。上限時の欠落量とtruncatedを記録する |
| A30 | 成果物回収中の切断 | collectingを保持し、回収だけ再開。計算をやり直さない |
| A31 | Unit結果欠落・空結果 | successにせず、欠落とno_workを区別する |
| A32 | dirty snapshotの成功 | 元HEADへのGitHub successを投稿しない |
| A33 | 必須secret／tool不足 | 実行前に診断し、stepを成功扱いでskipしない |
| A34 | 親からinstall・upgrade・rollback | 失敗から再開でき、実行中binaryを上書きしない |
| A35 | user／system-user service | 宣言した起動条件で復帰し、実際のservice環境でdoctorが通る |
| A36 | 既存CLI互換 | 旧コマンド、status JSON、主要workflowの動作を維持する |
| A37 | 実際に使うプロジェクトのCI | 同一snapshotで親のみ／複数Macの結果が一致し、分散の実益を確認できる |
| A38 | needs連鎖の異なるworker配置 | 先行Jobの結果とoutput／artifact確定後に別hostで後続Jobが始まり、重複実行しない |
| A39 | Jest／Vitest等のファイル単位shard | test discoveryとfilterが一致し、重複・漏れなく合算できる |
| A40 | 履歴を使う不均等テスト分割 | slow testを考慮してmakespanを短縮。履歴なしでも決定的fallbackで実行できる |
| A41 | shardの不適合・サービス依存・失敗 | 分割不可を説明し、適切に単一Jobへ戻す。fail-fast／retry／reportが正しい |
| A42 | 版指定のtoolがworkerに無い／版が違う | 自動導入できるものは導入後に実行し、導入不能ならblockedと手順を示す。導入中の負荷をforecastに含める |
| A43 | secret_filesを持つrunをworkerで実行 | attempt workspaceに同じ内容・0600で存在し、terminal後にworkerから消える。object store・cache・template・log・artifactに残らない。worker再起動後は親から再送される |
| A44 | `workers diff`／`sync` | 親とworkerのtool版・PATH上のtool・belt版・Docker可否の差を正しく示し、syncが自動導入可能な差だけを解消する。belt版の行に、組み込んだtask-traffic-controller・local-ci・fast-worktree・portoolの版が添えて出る。workerのPATHにある単独CLIの版は差分にしない |
| A45 | Linux（x86_64・arm64）のworker | CIのLinux環境で、導入→systemd user serviceでの起動→停止が通る。OS依存部分（suspendを含む単調時計、boot ID、process identity、sleep抑止）がmacOSと同じ契約を満たす。system-user modeは非rootの `User=` で動く |
| A46 | 一時workerと `workers forget` | `worker --ephemeral` が起動ごとに新しいidentityを持ち、トンネル越しのSSHで配置できる。到達不能にしたattemptはunknownのまま保持して再投入しない。`workers forget` を実行したときだけinterruptedに確定してidentityを退役させ、遅れて戻った旧identityは接続を拒否され、submitされない |
| A47 | `.worktreeinclude` に一致するignoredファイル | secret_filesの宣言なしで、22.3と同じ経路（HMAC digest・0600・terminal時の削除）でworkerへ送られ、object store・cache・template・log・artifactに残らない。`[source] secret_files` には追加分だけを書けば足りる |
| A48 | fast-worktreeによるworkspace | fast-worktreeもportoolもPATHに無い（beltだけを入れた）workerで、attempt workspaceが組み込みのfast-worktreeで作られ、プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` がそのまま使われる。`pre_remove` の `portool release` とcommandの中の `portool exec` が組み込みの版で動く。terminal後にworkspaceとportoolの割当が消え、hookがreleaseを書いていない場合も割当が残らない。macOSとLinuxで通る（Linuxでは依存の複製が効かず `npm ci` になってよい） |
| A49 | priorityの異なるworker | 予測完了時間の差が補正量未満なら高いpriorityのworkerに配置し、補正量を超えて速いworkerがあればそちらに配置する。全員同じpriorityなら従来と同じ配置。配置理由に補正量が出る |
| A50 | residentなしの `belt exec` と単独task-traffic-controller | coordinatorもworker agentも動いていないhostで、`belt exec -- <command>` が呼び出し時のcwdで動き、runを作らない。単独の `task-traffic-controller exec` と同時に流すと、同じ台帳・scope・policyで1つの予算を共有し、admissionの合計が予算を超えない。commandのexit codeがそのまま返り、Ctrl-Cでcommandが止まって台帳のentryが外れる。`--snapshot` と `submit` は理由を示して断られる。residentが動いていてworkerが無ければlocal workerで動き、workerがあっても `--local` なら必ずlocal workerで動く。どちらもadmissionを通る。組み込みcoreが読めない台帳の版は書き換えずに断られ、`doctor` が理由を示す |
| A51 | どのメンバーからでも投入できる | 2〜3台のメンバーが同じグループに属し、互いを `[[workers]]` に持つ。あるメンバーで `belt ci` を実行すると、そのメンバーの自分のworkerと他のメンバーの両方で実行される。runの記録は投げたメンバーのDBだけにあり、他のメンバーの `ls` には出ない。別のメンバーから同時に投入したrunも完了する。既にbeltを持つhostへの `workers add` は2つ目のworkerを作らない。`workers add --install` で導入されただけのhostは、`init` するまで仕事を受けるだけで、投入はできない。1台だけの `init` は従来どおり動く |
| A52 | 複数のメンバーが1つのworkerを取り合う | 2つのメンバーが同じworkerへ同時に配置しても、そのworkerのterminalでないattemptの合計が、worker自身の設定の受付窓を超えず、task-traffic-controllerの予算も1つである。窓を超えたsubmitは容量を理由に断られ、workerにはtombstoneを含めて何も残らない。1回だけ送って断られたattemptはretireなしで閉じ、送信の結果が不確かなattemptはretireの確認の後に、別のworkerへ置き直す。user commandは最大1回だけ開始され、遅れて届いた元のsubmitは `RETIRED` で断られる。置き直す先が無いときは、taskは失敗せず `task_waiting`（`capacity`）で待つ。自分のlocal workerの窓が他のメンバーの仕事で埋まっているときも、自分のrunは失敗せず、他のworkerへ置くか待つ（自分の仕事の優先枠は無い）。forecastと `estimate` は他のメンバーの需要を含み、その件数を示す。あるメンバーのepochが進んでも、他のメンバーの要求は `STALE_EPOCH` にならない。別のメンバーが同じ `attempt_id`・`task_id` を使っても、互いの仕事を `RETIRED`・`CONFLICT` にせず、状態も読めない（正直なメンバーの前提で、取り違えを防ぐ）。別のメンバーのattemptが持っている `attempt_id` へのsubmitは、何も保存せずに `attempt_id_in_use` を理由に断られ、相手のattemptは変わらず、断られた側のtaskは失敗せずに新しい `attempt_id` で配置し直される |
| A53 | グループ鍵、メンバーの削除、退役の通知 | グループが違う接続と、鍵が合わない接続は断られ、attemptを受け付けない。proofの前に、workerはidentity・epoch・能力を返さない。接続した側は、workerのproofが合わなければ仕事もsecretも送らない。名前と `coordinator_id` の組が記録と違う接続は断られる。`workers add` は、別のグループのマシンを `--move` なしでは移さない。`group rotate-key` は、他のメンバーの一覧に自分の知らないメンバーが居れば配る前に断り、配る段では動いている仕事に何も起こさず、切り替えた後は古い鍵の接続を閉じて断る。残るメンバーのattemptはleaseの範囲で続く。配る段・切り替える段のどこで落としても、再実行が続きを行う。次の鍵を持つメンバーは断られた時点で自分で切り替わり、持たないメンバーは `group send-key` で戻る。別の指紋の次の鍵がある間、2つ目の更新は断られる。`--remove` はSSH鍵を消す手順を表示し、外したメンバーへSSHで到達できる間は `group`・`doctor` が警告する。外したメンバーが投げたattemptは、残るworkerがleaseの失効で止める。外したメンバーの上のattemptはunknownのまま保持され、leaseと停止猶予が過ぎる前の `workers forget` は断られ、過ぎた後は `interrupted` に確定する。forgetしたメンバーを `send-key` で戻すと新しいworker identityで接続し、古いidentityのattemptは送られない。退役済みidentityのworkerが戻ると、投げた側はattachせずに退役を通知し、workerはそのメンバーのattemptだけをleaseの失効を待たずに止める |
| A54 | 投げたメンバーの停止 | あるメンバーのresidentを止めると、そのメンバーが投げたrunだけが進まなくなり、そのattemptはleaseの範囲で続いた後に止まる。他のメンバーが投げたrunは同じworkerの上で完了する。止めたメンバーを起動し直すと、同じattemptをadoptし、再spawnしない。他のメンバーの仕事を持つマシンへの `workers stop` は、`--all-submitters` が無ければ断られる |
| A55 | 組み込みのfast-worktree・portoolと単独CLIの共存 | 同じhostで単独のfast-worktree・portoolとbeltのattemptを同時に動かしても、state directory・lock・registryが壊れない。どちらが割り当てたポートも他方から見え、同じポートが二重に割り当てられない。単独CLIのcache・pool・trashをbeltが壊さず、逆も同じ。組み込みの版が読めない形式は書き換えずに断られ、workerは `blocked: environment_mismatch` になって `doctor` と `workers diff` が両方の版を示す。`workers diff` は組み込んだ版を示し、PATHの単独CLIの版は差分にしない |
| A56 | 遅い・応答しないメンバー | workerへの呼び出しが1つ、永久に答えないままになっている状態で、自分のDBだけで答えられるCLIの要求が完了し、他のworkerのlease更新と配置が進む（答えない呼び出しの完了を待たないことを、構造として確かめる）。加えて、その要求が仮想の時計で2秒以内に返る。受ける側でも、答えないメンバーの接続を持ったまま、他のメンバーの要求が完了する。sleepに頼らない決定的な試験で確かめる |
| A57 | メンバーの更新とrollback | `belt upgrade` でマシンを更新しても、そのメンバーが投げたrunと、他のメンバーから受けたattemptを失わない。commitの前は、新しい版が配置・generationの作成・回収の確定を行わず、他のメンバーからの状態を変える要求には、何も保存せずに「後で送り直せ」と答える。更新のための保留は `workers resume` では解けない。期限までにcommitされなければ、新しい版が応答していなくても、serviceの入口が親DBとworker DBを一緒に元へ戻し、元の版が同じattemptをadoptして、user commandを再び開始しない。試用中に別のメンバーが、更新中のマシンの上のattemptをretireしようとした場合：retireは相手の側で未確認のまま残り、rollbackの後に届いてtombstoneになり、そのattemptも置き直した先も二重に走らない。NとN+1のメンバーが、投げる側・受ける側のどちらの向きでも動く |
| A58 | 既存のworker DBの移行 | liveなattemptを持つworker DB、または終了したがresultが回収されていない（`ack_collected` を受けていない）attemptを持つworker DBの移行（step 4）は、両方の件数と手順（前の版でdrainし、終了と回収を待つ）を示して断られ、DBは変わらない。drain済みで、liveなattemptも未回収のresultも無いworker DBは移行でき、古いattempt・tombstone・lease・epochの行は捨てられ、捨てたattemptのdirectoryは次の起動で消され（attemptsのdirectory直下の、捨てたIDと同じ名前のものだけ。symlinkはたどらない）、グループのIDと鍵は世代1として引き継がれる。移行後、古い版の接続は `group_proof` を通れず、submitできない |

A50は第4章・第16章の `belt exec`、A55は22.5の組み込みの受入である。どちらも1台のhostで、単独CLIを同時に動かす試験で確かめる。A51〜A54とA56〜A58は第24章の受入で、1台のhost上に2〜3個のメンバー（別のstate directory）を立てた試験で確かめ、実機間の確認（別マシンの停止、実SSHでの鍵の配布）は工程6に含める。

A37では、実行時間を準備・転送・資源待ち・計算・回収に分けて記録する。小さいjobは転送で遅くなる場合もあるため、全jobの一律speedupを保証値にしない。対象CIの分散可能な並列幅、cold／warm、親単独との差を報告し、目標の負荷が1台へ集中したままなら完成としない。

CI自体のpassには、必要なtaskが仕様どおり成功または許容されたskipとなり、unknown・incomplete・必須artifact欠落がなく、結果が親に永続保存されていることを要求する。

# 20. 実装工程と完了の扱い

工程は依存関係に沿って分ける。機能を削った別の完成版を途中で設定しない。

| 工程 | 作業 | 完了を確認する対象 |
| - | - | - |
| 1 | core抽出と互換境界 | 旧CLI、共通budget、explicit context、signal分離 |
| 2 | sourceとFrozenPlan | dirty入力、secret bundle、Git文脈、Job DAG、ExecutionTask／Shard、setup recipe、artifact宣言 |
| 3 | durable runtimeとprotocol | receipt、supervisor、lease、cancel、idempotency、spool、OS依存部分（macOSとLinux） |
| 4 | forecastと動的配置 | WorkloadPlan、overlay、bounded dispatch、途中worker参加 |
| 5 | CI統合と運用 | 自動テストsharding、runner adapter、結果完全性、status、service導入・更新（macOS・Linux・一時worker）、toolchain整合、組み込みのfast-worktree・portoolによるworkspace、doctor |
| 5b | グループと投げたメンバー | グループ鍵の確認・配布・更新、投げたメンバーごとのepochと所有、調停と置き直し、退役の通知、メンバーの設定とCLI、メンバーの更新、遅いpeerの切り離し（第24章） |
| 6 | 受入検証と修正 | A01〜A58、実Mac、Linux、実プロジェクト、障害注入 |

完了報告には、実装commit、対応version、検証したMac・Linux・OS・profile、受入結果、既知の互換制限、運用手順を添える。未実装の必須項目を「将来対応」と記載して実用版完成とはしない。

この仕様で中心となる判断は、**入口をbeltに一本化し、local-ciのCI知識とtask-traffic-controllerのホスト内資源制御を維持しながら、分散と永続実行をbeltが所有すること**である。利用者は `belt ci` または `belt exec` で投入し、実行先の選択と復旧は同じ仕組みへ任せる。

# 21. 調査基準と参照

調査時点の公開mainを固定して読んだ。ここに記載した変更APIやbeltの挙動は実装提案であり、以下の基準版の既存機能ではない。

| 対象 | 基準 |
| - | - |
| local-ci | 調査時点 v0.6.2 / `873b4a72cc1e4f30d1e538cbfabd6c821682d08f`。lib化後の基準 0.9.0 / main `8666da7`（タグ `v0.9.0`） |
| task-traffic-controller | 調査時点 v0.4.2 / `8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713`。lib化後の基準 0.5.0 / main `18b2d45`（タグ `v0.5.0`） |

既存挙動の根拠は第16・17章のcommit固定source linksを参照する。特に [local-ciのsetup計画](https://github.com/t09tanaka/local-ci/blob/873b4a72cc1e4f30d1e538cbfabd6c821682d08f/src/plan.rs)、[task-traffic-controllerのadmission](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/admit.rs)、[task-traffic-controllerのlocal lease](https://github.com/t09tanaka/task-traffic-controller/blob/8fdeb83ecd8b68efcd87ddd1d52ef9d643b79713/src/lock.rs)が統合の重要な境界である。

Macのservice運用、SSH、Git転送の根拠は次の一次資料とする。ここからbelt固有のlease・state machine・再配置方針が自動的に得られるわけではなく、それらは本仕様の設計判断である。

- [Apple Remote Login](https://support.apple.com/ja-jp/guide/mac-help/mchlp1066/27/mac/27)：SSH／SFTPと利用userの設定。
- [Apple Designing Daemons and Services](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/DesigningDaemons.html)：daemonとagentの実行文脈。
- [Apple Creating Launch Daemons and Agents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)：loginとservice lifecycle。
- [Apple launchd.plist manual](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5)：UserName、KeepAlive、process group等のservice設定。
- [OpenSSH ssh manual](https://man.openbsd.org/ssh.1)：リモートcommandとSSH transport。
- [Git bundle documentation](https://git-scm.com/docs/git-bundle)：Git objectsとrefsの転送・検証。

# 22. 環境の整合

第5章の能力照合は、合わないworkerを候補から外すだけである。それだけでは追加したworkerが恒久的に「使えないworker」のままになり得る。本章は、worker間の環境差を検出し、自動で揃えられるものを揃え、揃えられないものの理由と手順を示す仕組みを定める。

## 22.1 toolchainの宣言と自動導入

plannerは、source snapshotに含まれる版指定を読み、実行要件の `ToolchainRequirement` にする。初回版で読むのは次とする。

| 版指定 | 対象 |
| - | - |
| `.node-version`、`.nvmrc`、`package.json` の `engines`／`packageManager` | Node.js、パッケージマネージャ（corepack） |
| `.tool-versions`、`mise.toml` | 記載された各tool |
| `rust-toolchain`、`rust-toolchain.toml` | Rust |
| `.fvmrc`、`.fvm/fvm_config.json`、`pubspec.yaml` の `environment` | Flutter／Dart |
| `.xcode-version` | Xcode |
| workflowの `setup-*` actionの `*-version` 入力 | 該当tool |

版指定ファイルの無いtoolで、workflowやprofileが必要とするもの（例: 版を固定していないFlutter・Xcode）は、親でcapture時に解決された版を、そのrunの `toolchain_baseline` として記録する。照合規則はプロジェクト設定 `[toolchain] match = "exact" | "minor" | "major" | "any"` で選び、既定は `exact` とする。baselineと合わないworkerは候補から外すか、自動導入できるなら導入する。

`ci` のtaskも、通常のtaskと同じく、提出時に固定したtoolchain（`FrozenBundle`）を持つ。宣言した版を用意できるworkerが無ければ、そのtaskは配置されない。配置の判断に残る理由の行は、`toolchain <tool>=<version> unavailable` の形（例: `toolchain node=20.0.0 unavailable`）とする。

baselineが分からないtoolの要求（profileの `required_tools` と、`requires` に挙げたtoolのうち、提出時にこのhostで版を測れなかったもの）は、提出時に `BLOCKED`、`environment_mismatch` で拒否する。理由は `toolchain baseline unknown; capture requires an observed required tool (<tool>, ...)` とし、配置時の `no_compatible_worker` にはならない。

公開 command submit は検証済み capture から版指定・必要 baseline を固定し、完全な resolution evidence とともに `FrozenBundle` として保持する。worker-local registry・既存 setup admission/singleflight・attempt ごとの activation 経路は [private adapter API](toolchain-adapters.md) に記録する。実 manager による受入確認と installed-version 一覧・diff/sync は別の検証範囲とする。

workerのprofileは版管理ツールadapter（nodenv、rustup、mise、fvm、corepack）を持つ。adapterは `detect`（導入済み版の列挙）、`resolve`（要件を具体版へ）、`install`、`activate`（そのattemptのPATH・envへの反映）を備える。不足版の導入は、第8章のsetupと同じ経路（task-traffic-controllerのadmission、worker内singleflight、導入済み版のworker内再利用）で行い、導入にかかる時間と負荷をforecastの準備時間に含める。導入はattemptのworkspaceではなくworkerのtool領域に行い、同じ版の並行導入を1回にまとめる。

管理者権限・ライセンス同意・GUI操作が必要なもの（Xcode本体の導入とライセンス同意、Docker Desktop、Homebrewのsystem package、macOSの権限）は自動導入しない。`blocked: environment_mismatch` とし、不足内容と実行すべき手順を示す。自動導入に失敗した場合も同じとし、成功扱いや別版での実行に切り替えない。

## 22.2 `workers diff` と `workers sync`

`belt workers diff [<worker>] [--json]`（workerを省略すると、登録した全workerへ同時に尋ね、結果は登録順に出す。同時に尋ねるのは最大8台）は、親（local worker。commandを実行したメンバー自身のworker）と対象workerのprofile実効環境の差を示す。比べる基準は実行したメンバーなので、別のメンバーから実行すれば差の出方は変わり得る。対象は、各toolの版（版管理ツールに導入済みの版一覧を含む）、PATH上の必要tool、belt自身の版（組み込んだtask-traffic-controller core・local-ci core・fast-worktree・portoolの版を添えて示す。22.4）、Docker実行可否、利用可能disk、profile digest、task-traffic-controller policy digestとする。項目ごとに `same / differs / missing / auto_installable / manual_required` を示す。`doctor` はこの結果を含む。

`doctor` は worker ごとの差を `environment_diff` として項目ごとに1 checkで載せ、区分は次のとおりとする。利用者が手を動かさないと直らない差だけが `fail`（exit 1相当のblocked）になる。
- `fail`: `differs`、`missing`、`manual_required`、diskの不足。
- `notice`（exitに影響しない）: `auto_installable`（execのウォームアップと `workers sync` が自動で入れるため。`workers sync <worker>` で今すぐ入れられると案内する）、親かworkerのどちらかが測れなかった項目（`not_measured`。例: scopeが決まらない、configが読めない。「まだ測っていない」と表示する）。policy digestは、親もworkerも、保持したservice環境が決めるtask-traffic-controllerのscopeの実効config（無ければ既定のpolicy）から測り、task-traffic-controller自身の `policy_digest` と同じ計算で出す。両方が測れて違えば `differs`（fail）とし、同じなら `same` とする。
- `not_checked`（healthyにならない）: 到達できなかったworker、worker・親の環境を測れなかった場合。親は `workers diff` と同じく保持したserviceの環境で測り、呼び出したshellの環境では測らない。

`doctor` は、登録した全workerを同時に見る（最大8台。出力は登録順）。1台の中でも、doctor helperの答えを得た後の bridge `hello` と環境の比較は同時に尋ねる。答えなかったworkerには環境を尋ね直さない。

`belt workers sync <worker> [--project <dir>]` は、対象workerに対し、自動導入可能な差を事前に解消する（ウォームアップ）。`--project` を付けた場合は、そのプロジェクトの版指定・baselineを基準にする。読むのは、指定したdirectory直下の版指定ファイル（22.1の表）と、`.ci/workflows`・`.github/workflows` 直下の `.yml`・`.yaml` のworkflowにある `setup-*` actionの `*-version` 入力（`*-version-file` が指すファイルを含む）で、どれもsymlinkをたどらず、1つ256KiB以下とする。同じtoolの版が別のjob・stepで違えば、workerはそれぞれの版を持つことを要求にする（matrixの版を全部入れる）。`${{ ... }}` の式のままの版は導入せず、固定の版を書くよう案内して blocked にする。同じ要求と同じ blocked は1つにまとめる。projectの宣言とworkflowの宣言が矛盾すれば、場所を付けて失敗する。手動対応が必要な差は導入せず、手順を表示する。sync中もworkerは新規配置を受けられるが、導入対象の版を要する配置はsingleflightで導入完了を待つ。

導入は、動いている resident が worker の link 越しに `env_sync`（第11章）で依頼する。resident が動いていなければ、起動せずに止まる。worker が `unchanged`（既に導入済み）と答えた版は、導入したものとして表示しない。導入した版も `unchanged` の版も、導入後に worker を測り直し、見えなければ blocked にする。導入を待っている間の親の中断（Ctrl-C）は、待つのをやめる要求である。親は次の確認の前に、tool を含まない `env_sync` で worker の warm-up を手放し（1回だけ試す）、「待たなくなった。他に待つものが無ければ導入は止まる」と `workers diff` の案内を出して、終了コード130で終わる。worker がこの要求を知らない（古い）ときと、要求が届かなかったときは、worker が最長 `warmup_idle_ms` 続けてから手放す。その間に同じ版を sync し直せば、同じ導入に合流する。導入を待っていない間の Ctrl-C は、これまでどおりその場で終わる。

失敗した導入と途中で止められた導入は、その版の private root を receipt の無いまま残す。この root は、次に同じ版を導入するとき（sync でも、仕事の中の導入でも同じ経路）に、導入者が root の `install.lock` を排他で取ったうえで中身を消し、初めから入れ直す。途中の中身は再利用しない。消す条件は次のとおりとする。

- 消すのは receipt の無い root の中身だけで、`install.lock` は残す。receipt のある root は、receipt が検証できない場合（壊れている、他人が読める、symlink である等）も含めて消さず、error にする。
- 消す前に、root が worker の toolchain 管理ディレクトリ（`<state>/toolchains`。導入者が呼び出し元から受け取り、root の親がこれと一致することを確かめる）の直下にあり、名前が導入計画の identity（64桁の小文字16進）で、自分だけが読み書きできるディレクトリであり、途中に symlink を挟まない path であることを確かめる。満たさなければ何も作らず、何も消さない。確かめた管理ディレクトリと root は開いたまま持ち、以後の操作はその descriptor を基準に行う。確認のあとで root の名前が symlink に差し替えられても、消すのは確かめたディレクトリの中身だけである。
- 中身は、まず管理ディレクトリの中の trash（`.trash-<root名>-<ULID>`）へ rename し、そのあとで消す。`install.lock` は rename しない。rename できなければ何も消さずに導入を失敗にする。こうすると、止めた導入者の process group を抜けて生き残った子孫がまだ書いていても、それは trash 側に書き、新しい導入は空の root から始まる（root の中を path で名指しして新しく作る書き込みまでは防げない）。
- trash は descriptor を基準に消す（symlink を辿らない `openat`、`fstat`、`unlinkat`）。path を使った chmod とディレクトリの読み取りはしない。symlink は辿らずに外す。ファイルは mode を変えずに外す（root の外のファイルへの hard link でも、外のファイルは mode も中身も変わらない）。mode を変えるのは、中身を消すために開けたディレクトリだけで、開いた descriptor に対して行う。読めないディレクトリは、空なら外し、空でなければそこで止める。root と違う device のディレクトリ（mount）に当たったら、中に入らずにそこで止める。
- rename が済めば root は空なので、trash を消し切れなくても（生き残りがまだ書いている、読めないディレクトリがある）導入は続け、trash は残す。別の device に当たった場合だけは導入を失敗にする。残った trash は、同じ root の次の clear と worker の起動時にもう一度消す（起動時は、その root の `install.lock` を待たずに取れた場合だけ）。
- 別の導入者が lock を持っている間は、何も消さずに lock を待つ。待った後に receipt があれば、それを結果とする。
- worker 内では、同じ版の導入者は同時に1つだけで、止めた導入者とその子プロセスの終了を確かめるまで次を始めない（第8章の setup と同じ）。

導入そのものが失敗した版は、worker が 10 分間覚える。覚えるのは次の場合だけである: 版 manager のコマンドが失敗した（0 以外の exit・signal）、導入後の probe が求めた版を答えなかった、導入先に別の device が mount されていた（この 3 つは導入の helper が専用の exit code 3 で知らせる）、導入者が動いている間（phase が running）に制限時間を使い切った。その間に同じ版を求めた別の仕事には、導入をやり直さずに、同じ失敗と再試行できるまでの秒数を返す。それ以外の失敗（承認待ちが切れた、承認を待つうちに制限時間を使い切った、lock・片付け・receipt の書き込みの失敗、helper が成功した後の検査の失敗）と、中断・cancel・導入者の消失や signal による終了は覚えず、次の仕事が導入をやり直す。覚えていない失敗には再試行までの秒数を付けない。覚えた失敗は、版 manager とその依存の identity が変わったとき、`workers sync` がその版を求めたとき（sync は必ず導入を試みる）、worker の再起動で解除する。setup の template の失敗（第8章）は、従来どおり覚えない。

## 22.3 secret_files（ignoredな入力ファイル）

テスト用の `.env` のように、gitで無視されているが実行に必要なファイルは、プロジェクト設定 `[source] secret_files`（glob、repo root相対）で宣言する。宣言したファイルは実行ごとにworkerへ送る（2026-11-09 決定）。

プロジェクトの `.worktreeinclude` に一致するignoredファイルも、`secret_files` へ書かなくても、自動で同じ経路（以下のcapture・HMAC・0600・terminal時の削除）でworkerへ送る（2026-11-10 決定）。`[source] secret_files` には、`.worktreeinclude` に無い追加分だけを書く。両方に一致するファイルは1つのbundle要素になる。`.worktreeinclude` が一致させるのはignoredなファイルだけで、tracked fileは通常のsnapshotに含まれる。

- **capture**: source captureと同じ検証手順（再検証と最大3回の再試行）で内容を読み、通常のobject storeとは別の `SecretBundle` にする。manifestにはpath一覧と、clusterごとの鍵によるHMAC digestだけを記録し、平文のhashは記録しない。一致するファイルが無い `secret_files` のglobは警告し、`required = true` を付けたものが無ければcaptureを失敗させる（`.worktreeinclude` の一致が0件でも失敗にはしない）。
- **親での保持**: 親のstate directory内に0600で保存し、runがterminalかつ必須回収が終わった時点で削除する。retryは同じbundleを使い、現在のファイルを読み直さない（snapshotの不変性を保つ）。
- **公開途中の停止からの回復**: 親はbundleを一時ファイル（`.<公開名>.<ulid>.tmp`、0600）に書き、同じdirectory内のlinkで公開してから一時名を消す。この途中で親が止まると、平文の一時ファイルが残る。link後に止まった場合は公開済みbundleの名前が2つになり、読み取りは安全でないものとして拒否し、次のVaultの保持ページで回復する。親の常駐loopは、Vaultの排他lockを取った保持ページの先頭で、この一時ファイルを1ページ最大32件消す。一時ファイルを書く処理は必ずVaultのlockを持つため、排他lockを取れた時点で残っているものは書き手のいない残骸であり、経過時間や時計は条件にしない（時計のHoldや未登録の旧入力による保留より前に行う）。消すのは、名前が厳密に一致し、symlinkを辿らずに見て自分が所有する0600の通常ファイルで、名前が1つ、または2つでもう一方がその一時ファイルの公開先であるものだけとする。それ以外は残して数え、一時ファイルをbundleへ昇格させない。1件を調べられない・消せない場合も、その1件を残して数え、同じページの残りと保持の処理を続ける（ページを失敗にするのは、Vaultのrootが開いた時のものでなくなった場合と、directoryの列挙・fsyncの失敗だけ）。残した一時ファイルの数は保持の失敗とは別に数え、常駐のログには最初の1回だけ出す。親の分類を記録するファイル（`parent-retention.json`）は、初回の登録の公開途中で親が止まると名前2つのまま残る。分類の正本はVaultのlockファイルに書いたpinで、これはファイルの公開より前に書いてfsyncしてある。そのため、pinがあり、もう一方の名前が同じdirectoryの自分の一時名（`.parent-retention.json.<ulid>.tmp`）で、symlinkを辿らずに見て同じinodeである場合に限り、このファイルは名前2つのまま読む（内容がpinと一致することは従来どおり確かめる）。Vaultを開く処理は読むだけで何も消さず、一時名は次のVaultの保持ページが上の規則で消す。pinが無い、もう一方がこの形の名前でない、名前が3つ以上、0600でない、のいずれかなら従来どおり拒否し、Vaultを開くエラーにその理由を含める。run bundleは名前2つのままでは読まない。
- **転送**: 認証済みSSH接続上のprotocol操作で、attemptごとに送る。workerは受け取った内容をHMAC digestで検証する。
- **workerでの配置**: attempt workspaceの該当pathに0600で配置する。workerのobject store、prepared template、cache、log spool、artifact、outputsに保存しない。template keyやcache keyにはHMAC digestを含め、内容の違うbundle間でcacheを共有しない（第8章）。
- **消去**: attemptがterminalになった時点で、ログ回収の完了を待たずに削除する。worker起動時のreconcileでも、terminal済みattemptに残るsecret_filesを削除する。
- **復旧**: worker再起動などで実行前にbundleが失われた場合は、親が同じbundleを再送する。親がbundleを失っている場合は `blocked: secret_unavailable` とし、空のファイルや現在のファイルで黙って再開しない。
- **ログ**: bundleに含まれる `KEY=VALUE` 形式の値を、第14章のmask対象に加える。workerでは、agentがsecret bundleの全ファイルの配置を確認した後、commandを開始する前に、配置済みのファイルから値を読み取り（memoryに保持しないので、bundleの到着後にagentが再起動しても同じ値になる）、supervisorのspecに入れて渡す。supervisorはcommandの出力をspoolへ保存する前に、すべてこのmaskに通す（出力の区切りをまたぐ値も置き換える）。値を運ぶspecのファイルは0600で書き、attemptの終了時にworkspaceとともに消す。8 bytes未満の値はmaskしない（短い値を置き換えると無関係な出力まで壊すため。該当した数をevent `agent_mask_skipped` に残す）。配置済みのファイルが読めない、配置時の状態でない（通常の0600ファイルでない、linkを経由する）、または値がUTF-8の文字列でない場合は、maskできないままcommandを開始せず、attemptを失敗にする。
- **status**: secret_filesは公開commitの内容ではないため、第17章の公開commitとの一致判定を妨げない。

## 22.4 worker上の周辺tool

task-traffic-controller runtime、local-ci core、fast-worktree（0.7.0）、portool（0.14.0）はbeltバイナリに含める（第3章）。どれもprofileの必要toolとして検査せず、自動導入もしない。組み込んだ版はbeltの版で決まるので、`workers diff` はbelt自身の版の行にそれぞれの版を添えて示し、別の差分項目にはしない。workerのPATHにある単独のCLI（別に入れた `portool` など）の版は、attemptの実行に使わない（22.5）ので差分にしない。

それ以外にプロジェクトが必要とするtool（版管理ツール、Dockerなど）は、22.1のとおりprofileの必要toolとして扱う。

## 22.5 workerのworkspace

workerのattempt workspaceは、beltに組み込んだfast-worktreeのライブラリで作り、消す（2026-11-10の決定。組み込みは2026-11-20の決定）。canonical cloneからattemptごとのworktreeを作る操作と、terminal後の削除に、fast-worktreeのadd・removeとpool・hookを使う。workerにfast-worktreeとportoolの単独CLIは要らない。

- **設定の源泉**: プロジェクトの `.fast-worktree.toml`（`[[clone]]`、`post_add`、`pre_remove` のportool release）と `.worktreeinclude` をそのまま使い、worker用の準備設定を別に持たない。`[[clone]]` と `post_add` が依存の準備を担う（第6章・第8章）。
- **hookとcommandが呼ぶ `portool`・`fast-worktree`**: `.fast-worktree.toml` のhook（例: `pre_remove` の `portool release`）や、commandの中の `portool exec` は、その名前のcommandを呼ぶ。beltだけを入れたworkerでも動くように、workerはstate directoryの中の私用のbin directoryに、belt自身を指す `portool` と `fast-worktree` のlinkを置き、hookとattemptのcommand、CI taskのstep、setupのstepのPATHの先頭に足す（PATHが無い、または空のときは、このbin directoryだけにする。空のentryはcurrent directoryを意味するので作らない）。beltは起動名（argv[0]）がこの2つのどちらかのとき、組み込んだライブラリの単独CLIの入口として動く。引数・出力・終了コードは同じ版の単独CLIと同じである。hookの中身をbeltが解釈することはしない。beltのsubcommand（`belt port` など）にもしない。workerのPATHに単独CLIがあっても、attemptの中では組み込みの版が先に見つかるので、`workers diff` が示す版と実際に動く版が一致する。linkは第15章のversion別directoryごとに置き、そのversionのbeltを指す。更新とrollback（24.8）ではversion別directoryと一緒に切り替わり、動いているattemptは開始したときのversionのlinkを使い続ける。
- **割当の解放**: terminal後の削除では、`pre_remove` のhookを通常どおり実行し、その後で、そのworkspaceに紐づくportoolの割当を組み込みのportoolで解放する（解放済みなら何もしない）。hookがportool releaseを書いていないプロジェクトや、hookが失敗した場合も割当を残さないためである。解放するのは、workspaceのdirectoryが実際に無くなったときだけで、まだ残っているworkspaceの割当は残す。解放に失敗したら、workspaceの削除の失敗と同じくcleanupのeventに残し、workerの起動時のreconcileでやり直す。
- **単独CLIとの共存**: 同じhostで単独のfast-worktree・portoolを使っていても、組み込みの版は同じstate directory、lock、registry（portoolの割当表、fast-worktreeのcache・pool・trash・kept ref）を、単独CLIと同じ規則で使う。どちらが割り当てたポートも他方から見え、同じポートを二重に割り当てない。組み込みの版が読めない形式（新しい単独CLIが書いたもの）は書き換えずに断り、そのworkerを `blocked: environment_mismatch` にして（portoolのregistryの版が組み込みの版で読めないときに限る。attemptの準備でも、workspaceの割当の解放でも同じ）、`doctor` と `workers diff` が単独CLIとbeltの版を示す。組み込みの版（fast-worktree 0.7.0、portool 0.14.0）が持つ自動の保持（古いkept ref・cache・spareの削除と、無くなったworktreeの割当の自動解放）は、attemptの中の `fast-worktree` の呼び出しがそのまま行い、beltは別の掃除を持たない。fast-worktreeの裏の掃除（`cache retire`）は、beltのlinkを起動した版が自分を指す `fast-worktree` として再実行するので、beltだけを入れたworkerでも動く。beltはその出力（削除した行）をeventにしない。beltは `portool prune` と `sync` を呼ばない。beltのcanonical cloneとattempt workspaceはworkerのstate directoryの中にあり、利用者が単独のfast-worktreeで作ったworktreeとは混ざらない。
- **`.worktreeinclude`**: 一致するignoredファイルは、親でcaptureして22.3の経路で送る。workerがcanonical cloneからコピーすることはしない。
- **Linux**: clonefileが効かないため、npmの依存は毎回 `npm ci` になる。add・remove・pool・hookは同じく動く。組み込みなので、Linuxでも別のバイナリは要らない。
- **手元の開発環境**: 編集を続けるworktree、devサーバー、Composeのポート分離には、従来どおり単独のfast-worktreeとportoolを使う。beltはこれらを扱わない（第3章）。

# 23. 設計判断と基準実装の更新（2026-11-09）

工程1のlib化で分かった事実に基づき、次を決める。

1. **supervisorは別プロセス**: attempt supervisor（とその配下のcommand supervisor）は、attemptごとに同じ `belt` バイナリを別プロセスとして起動する。local-ci coreとtask-traffic-controller coreは「1プロセスで1 attempt」として呼ぶ。理由は、lib化後も両coreにprocess-globalな状態が残っていること（local-ciのkill追跡とworkflows dir既定値、task-traffic-controllerでは `wait4` が割り込まれるのがsignalを受けたスレッドだけであることと、ledgerのpidが `std::process::id()` であること）。ただし第17章のとおり、local-ciのkill追跡はattempt所有へ置き換える。プロセス分離はその上での二重の安全策とする。
2. **lib化後の基準**: task-traffic-controller 0.5.0（main `18b2d45`、タグ `v0.5.0`）、local-ci 0.9.0（main `8666da7`、タグ `v0.9.0`）。いずれもcrates.ioに公開済みで、beltはcrates.ioの版へ依存する。2026-11-09の決定時点の基準は local-ci main `7deff5e`（v0.7.0 + PR #21 によるlib化）、task-traffic-controller main `7f095ad`（v0.4.2 + PR #14 によるlib化）で、第3項・第4項の観察はその時点のものである。
3. **待ち時間の分離**: local-ci v0.7.0は、jobごとに資源待ちを実行時間と分けて記録する。第9章の実行時間には、この待ちを除いた値を使う。
4. **task-traffic-controllerの既知の窓**: PR #14の後、待ち中のrunnerがSIGTERMを2回続けて受けると、exit fileを書かずに終了し、jobがlostに見える短い窓がある。C3（受理段階のreceiptと終了順序）で、signal handlerを既定へ戻す時点をCLIの再送の直前へ移して閉じる。


### 公開ログの読み取り

`logs <run-or-task-id> [--follow]` は親が回収して commit した stdout / stderr の
prefix を読み、worker 接続・owner 起動・Git/project 設定を必要としない。
既定は非 retired の最新 generation（task の min_generation 以上）であり、retry 待ちに
旧 attempt へ戻らない。`logs <task-id> --attempt <attempt-id>` で同 task の履歴を明示できる。
wire の node_id は公開 CLI の選択単位にせず、attempt 集約 stream を読む。

text は任意 bytes を変換せず対応する stdout / stderr へ出す。識別・回収状態の診断は
stderr へ出す。stream 間の時系列順序は保証しない。`--json` は snapshot / follow とも
schema_version=1 の NDJSON で、data.kind は status / replacement / reconnecting / chunk / final。
chunk は run_id / task_id / attempt_id / generation / stream / offset / next_offset /
bytes_base64 を持ち、生 bytes は JSON stdout に混ぜない。chunk は最大 64 KiB、
run は最大 32 task のページで走査する。final は件数・bytes・pending / failure の集計であり、
全 task の配列を保持しない。読み取りエラーは既存の error envelope を返す。

`ci` のrunを対象にしたときは、commandの出力のあとに、記録されたGitHub statusを、`ci` のtext出力の `status:` 行と同じ文言の1行（例: `status: <context>=<state> posted to <sha>`）としてstderrに出す。`--json` ではstatus recordの代わりに `kind` が `github_status` のrecord（`run_id`、`github_status`）を出す。statusの投稿が記録されていないrun、およびtaskを対象にしたときは何も足さない。`--follow` では、runが終わって回収が閉じたときに1回だけ出す。

follow は invocation 専用の mode-0700 一時ディレクトリ / mode-0600 SQLite cursor に
配信 offset を保存し、メモリは task 数やログ量によらず制限する。終了時に削除し、
プロセスを跨ぐ resume は提供しない。owner の再起動でも read-only DB から同じ offset
を継続する。prefix の縮小は CONFLICT、commit 済みファイルの欠損・短縮は INTERNAL とする。
実行 terminal だけでは終了せず、collection closed と利用可能 prefix の配信完了を待つ。
closed の不完全ログ・回収失敗は利用可能 bytes を返して COLLECTION_FAILED / 78。
途中 snapshot は pending を明示して 0。Ctrl-C は cancel を送らず detach / 130。
出力先が詰まっている場合も detach は待たず、割り込み時の final は出力可能な場合に送る。

retention GC は run ごとの永続 pin の共有/排他 lock で reader と分離する。
read-only CLI は pin 作成・DB 更新を行わず、owner が準備した inode を検証して保持する。
active / pending / finalizing と reader 使用中の削除を防ぐ。ログ経路だけで
source/Vault/private binding GC や output planner の完成を意味しない。

### Generic output bundles (TarV1)

`[[outputs]]` の各論理名は1つの非圧縮 tar blob として回収する。`paths` は
workspace 相対の globset パターンの和集合であり、`*` は1要素、`**` は再帰、
`?`・文字クラス・brace alternation は globset の構文とする。literal directory は
全 regular file descendants を含み、重複は除く。member 名は workspace 相対のまま
保持する。通常の dotfile は対象となる。`.git` と runner metadata は走査しない。
選択された symlink、hardlink、FIFO、socket、device、secret path/inode/value、
case/NFC 名衝突は論理 output 全体を refused にする。source の captured input
manifest は出力 allowlist として使わず、新しく生成したファイルも回収できる。

`when = success` は Succeeded、`failure` は Failed/TimedOut、`always` は収集可能な
全 terminal outcome に適用する。Cancelled/Interrupted は always のみ。未起動の
cancel/prepare failure は収集せず、prepared-only success は収集する。条件に
合わない required output は required_missing にしない。条件は command outcome
で判定し、artifact 不足による verdict を再入力しない。

1 attempt は raw artifact と合わせて1024論理出力・1024 patterns、1 bundle は
4096 files、attempt 全体16384 files、走査は1 bundle 当たり100000 entries、
path は UTF-8 1024 bytes・16要素、member と serialized bundle は各最大1 GiB
（worker設定がより小さければその上限）とする。上限超過は切り捨てず refused。
各 member は held directory fd と NOFOLLOW/NONBLOCK で開き、64 KiB 単位で
private spool に secret 検査してから archive に追加する。bundle 失敗は他の
論理 output を妨げず、既存の required/optional 判定を維持する。

wire schema 1 に default/skip-empty の `outputs` を追加し、従来の `artifacts`
は raw のまま保持する。`ArtifactRecord.encoding` は raw を既定とし省略し、
TarV1 は明示 `tar_v1` とする。収集段階の到達は `outputs_evaluated` (default false/skipfalse) として journal と
result に固定する。missing/refused や zero eligible でも収集を評価した場合は true、
未起動 cancel/prepare failure は false とする。Succeeded/exit/Running receipt/
Running timeout と marker false が矛盾する結果は拒否する。CommandRegistered は
admission 前に立つので、その receipt 単独は開始証拠としない。
encoding と marker を含む immutable result JSON と digest が
既存の exact name/digest/size receipt・ACK を束縛する。旧 persisted payload/result
の canonical bytes と digest は変えない。全 live hello で `output_bundles_v1` を
必須とし、設定の required_features が空でも迂回できない。旧 peer との接続は
明示的な protocol_incompatible となる。doctor/version で同一 schema だけを見て
互換と判断せず、hello の必須 feature も確認する。

TarV1 writer は Rust tar crate の GNU regular-file header のみを出力し、mode0600、
uid/gid/mtime0、名前順とする。100 bytes 超の名前は同 crate が生成する bounded
GNU `L` (`././@LongLink`, UTF-8 member path + NUL) metadata を許可する。
compressed data、directory entries、link、special、sparse、PAX/global metadata は
生成しない。reader/export はこの subset と全構造・外側 digest を再検証し、
raw bytes を magic sniffing で tar と解釈しない。汎用 unpack は使用しない。


### Public collected output export

`outputs <run-or-task-id> --dir <directory>` exports currently receipted outputs. The destination is required. `--attempt <attempt-id>` addresses an exact historical attempt and is valid only with its owning task. Default selection respects the task's latest generation boundary; a retry awaiting placement never exports an old success. Reads work with the owner stopped, without Git or project configuration, and never start a service or change the coordinator epoch.

Legacy raw objects export at `<directory>/<task>/<attempt>/<name>`. Generic `tar_v1` bundles export at `<directory>/<task>/<attempt>/<name>/<workspace-relative-member>`. Encoding comes from immutable result metadata; raw bytes are never sniffed as archives. Durable artifact receipts plus fresh retained result/blob references authorize objects. Reads and private spool copies use bounded buffers, and metadata/task pages contain at most 64 entries.

The exporter verifies outer size and SHA-256, then validates the whole uncompressed GNU tar subset before publishing any bundle member. Only regular files and bounded GNU long names are supported. Unsafe paths, duplicate/case/Unicode/prefix collisions, links, sparse/PAX/special entries, truncation and trailing garbage are refused. Destination ancestors must be real directories; symlinks and existing destination files conflict, even when bytes match. New directories are 0700 and files 0600. Publication never overwrites. Concurrent destination conflicts preserve successful exports and report each failure.

`--json` uses bounded NDJSON: target observation, per-file outcome or artifact error, then exactly one final summary attempt with aggregate counts and a reader exit code. Target execution failure is independent of successful export. Pending collection is reported without claiming run success; closed collection failure exits 78. Expired, unreferenced or deletion-claimed data exits 73 even if the file still exists. Missing/short/corrupt files with live references exit 79, destination conflicts 72, invalid inputs 2. Ctrl-C exits 130 and detaches only the reader, including when stdout is blocked. The shared run pin is held through cooperative worker cancellation/drain and every final write/flush, including backpressure and SIGINT.
結果 retention GC は run ごとの永続 reader-pins inode と共有/排他 flock を使う。
read-only logs は対象 run を仮解決し、pin を read-only/nonblocking shared-lock し、
inode と新しい metadata binding を確認した後に読み始め、follow/detach を含む invocation 全体で保持する。
missing/legacy pin は明示 unavailable とし、reader は作成・DB 更新・owner 起動を行わない。
outputs/export CLI も configured read-only store の identity 検証後に CoordinatorStore::result_read_pin を取得し、
新しい metadata 読取から blocking worker の drain と最終出力/flush 完了まで invocation 外側で保持する。
owner は create_run/enqueue の両経路で DB transaction に入る前に pin 作成と fsync を行う。
ref 公開前にも pin を準備し、legacy run は bounded page で backfill する。
pin inode は run が在る間保持し、置換しない。run の刈り取りの最後（25.5 の段8）に、排他で取った owner が unlink する。pin を作るのは、行があって印の無い run についてだけで、行の無い run の pin は owner の起動時の走査が消す（25.11）。最初の GC 対応版以降の owner と
reader がこの protocol を共有することが互換条件で、旧 reader と同時利用する GC の安全性は保証しない。
resident は bounded keyset の結果 ref/claim page だけ処理し、run-exclusive pin を FS delete 完了まで保持する。
known durable claim のみ NotFound を冪等成功とし、未関連 legacy pending や foreign locator は削除しない。
run 選択は task facts/count を読まない run ID index の 1 件 keyset query とする。
削除は open 時の result root inode を確認し、各祖先を NOFOLLOW で開いた FD を基準に行う。
leaf symlink は参照先を辿らず unlink し、祖先/root の差替えは claim を残して失敗する。
空親/tmp の cleanup も同じ FD から行い、外部 symlink 先を削除しない。
GC 排他 pin と writer shared pin の通常競合は対象を pending として次 pass へ送り、
resident の収集 loop・lease 更新・IPC は継続する。pin 破損や他の I/O error は通常競合にしない。
通常競合でない結果 GC のページの失敗（消せないファイル、祖先の差替え、foreign locator など）でも resident は止めない。
claim とデータを残して失敗を数え、原因の path を出さずにログし、その run を次の周回で再試行する。その間もほかの run の
GC・収集 loop・lease 更新・IPC は続く。resident を止めるのは fence と owner を失った場合だけである。
時計 cutoff は boot identity と永続 monotonic baseline から導出し、boot 不明/変更後は全 retention の
quarantine、時計異常は retention 0 でも Hold。壁時計だけを理由に削除を前倒ししない。
source/Vault/private binding の GC と output planner 完成はこの結果 retention 経路に含まない。

state directory の path は、どの要素にも symlink を許さない。`init` と常駐の起動は、祖先を含めて 1 つでも
symlink があれば、state を作る前・DB を開く前に拒否する（path を解決して受け入れることはしない）。
エラーには symlink だった要素の path と、実体の path を指定する（state なら `HOME` か `XDG_STATE_HOME` を実体の
path にする）という案内を含める。`init` が調べるのは実際に使う path（LaunchAgents、設定、state）で、`XDG_STATE_HOME`・
`XDG_CONFIG_HOME` を指定した場合、使わない `HOME` 配下の既定の path は調べない。LaunchAgents は常に `HOME` の下に
あるため、`HOME` 自体が symlink を経由する場合は `HOME` を実体の path にする必要がある。
Source・Vault・結果の削除は root から祖先を NOFOLLOW で開き直して対象を確かめるため、起動後に祖先が
symlink へ差し替わった場合は、その保持ページは claim も unlink も行わずに失敗し、元に戻るまで何も消さない。

### Source retention roots (nondeleting foundation)

The source store owns a private permanent mutation-lock inode and a separate retention index. Capture takes a shared guard and persists a pending root before local-ci publishes any objects. Graph registration uses persistent manifest positions and at most 32 references per step, including submodule edges and shared blobs. A matching durable run root precedes pending-root handoff. Unsubmitted captures and unknown crash roots remain protected. Legacy registration is bounded and conservative.

Retry/enqueue acquire source guards before admission/actor/DB operations and retain matching guards through actual commit; forwarding retains its guard through blocking IO drain. Preview takes nonblocking exclusive ownership, validates the configured store and source/run binding, checks all generations and trusted monotonic retention, and reports at most 32 unique owned manifest/blob paths. Unknown, pending, malformed or shared roots remain held. Preview never expires a root, writes an unavailable tombstone, claims deletion or removes source/Vault files. The foundation itself enables no deletion; the D resident driver below now schedules Source/Vault retention. Owned Git/bundle cache cleanup and safe legacy abandonment remain follow-up work. Chapter 25 stage 2 (25.5 step 4) defines the Git and bundle cleanup; it is not implemented yet. Original workspaces and private in-place bindings are excluded.

Registration retains one validated manifest and its file/directory identity across 32-reference pages, finishing that manifest before selecting newly queued children even when their IDs sort before it, so pages never repeatedly parse/hash the full manifest. Foreground producers supply their already-held manifest; run binding copies the completed indexed membership instead of rereading it. Background legacy loads default to the receiver's 64 MiB manifest limit and use a measured-length bounded reader. An oversized legacy manifest produces typed `Preview.manifest_limit` Hold; callers may set `PreviewCursor.manifest_byte_limit` explicitly to resume registration within that chosen bound. Source usage remains available. Arbitrary-size automatic legacy streaming registration remains pending. The preview cursor holds at most one manifest, with memory proportional to that manifest within the chosen byte bound, never a whole source graph. Future expiry of received roots also requires per-import pending ownership instead of relying on the current hash-shared pending blob marker. Chapter 25 stage 5a (25.8) defines that per-import ownership; the deletion of received roots is specified by stage 5b (25.8) and not implemented yet.
### S20a: captured declaration API boundary

`toolchain::snapshot::read_captured` reads bounded, verified captured objects,
not the live checkout. `versions::effective` intersects declarations per explicit
project/workflow/job/step scope; contradictory declarations fail with source
locations. There is no implicit precedence between project declaration files.
The setup-node action's nonempty direct node-version does override its own
node-version-file, including when the direct input is an unresolved expression.
Action version-file paths are relative to the repository workspace, independently
of the selected project directory. Captured Node files are decoded as structured
JSON (Volta/devEngines/engines), mise TOML or node/nodejs-prefixed plain versions.
Deferred expressions and
manager channels require explicit resolution; missing observations remain unknown,
even with `match = "any"`. `FrozenToolchains` retains source/profile identity,
requirements, baseline provenance and explicit concrete selections for reuse on
retry. Every declared range/channel needs a concrete selection before the frozen
contract becomes resolved; a matching range alone cannot float on retry. Declared
concrete selections always use exact version identity. The configurable match
policy applies only to baselines for tools without a declaration. This API
is not yet part of submit/admission payloads.

Node-style ranges use the npm-compatible `node-semver` parser. Pub environment
constraints use Dart's distinct caret and prerelease-bound rules. Pub concrete
observations/selections include build metadata in ordering and equality; bounds
containing build metadata currently return an explicit resolution-required diagnostic. Channel,
workflow-expression and version-file-expression resolution is deferred, with no
host env expansion. A selection's authority digest is supplied by the trusted
parent/adapter; this API verifies its source/profile/scope/requirement binding,
not the external resolver evidence itself. Snapshot version documents must be
regular captured objects; secret entries and symlinks are rejected explicitly.

S20 still requires manager adapters (nodenv/rustup/mise/fvm/corepack), private
installation roots, resolution evidence, activation, installation singleflight
and submit/worker integration. S21 workers diff/sync/doctor and real service
environment acceptance remain mandatory. This declaration slice advertises no
auto-installable capability.

### Source import ownership and future erasure fences (nondeleting stage A)

Source import ownership is per opaque token and physical store identity, never per shared hash.
Publishing one import only releases its own matching memberships after its immutable graph is durable;
positive handoff to an exact durable run binding releases memberships in transactions of at most 32 rows.
Failures, unmatched blobs and unknown crash/legacy roots remain protected. Tokenless legacy intake keeps
independent protective roots. A does not infer abandonment from absence of coordinator references.

Run expiry and publication incarnation are distinct source-owned records. Content-addressed SourceID/hash
identity is unchanged: the same immutable input can be republished in a new incarnation after an old claim
completes, while the old run remains permanently expired. A only adds schema/checks; production creates no
expiry or deletion claim and removes no source/Vault file. Vault-only SecretUnavailable must remain separate
from source expiry and from source retention. Actual bounded deletion and resident activation remain B/C work.

Public SDK access is a shared-guard-owning view with no raw ObjectStore Deref/Clone. Trusted crate SDK calls
use a scoped callback whose entire execution holds SH; pending publication claims conservatively block raw
SDK operations. Opened upload readers own SH through actual blocking reads and cancellation drain. These
APIs do not claim to prevent intentional raw filesystem or SDK-handle escape by trusted callers. Publication,
binding, enqueue, retry preflight/commit, input read and delivery readiness check future availability fences,
with source ownership acquired before coordinator operations. The permanent lock and SQLite inode safety
protocols remain unchanged. No automatic source/Vault deletion is enabled by this stage.


### Source deletion page (B, scheduled by D below)

The parent source store exposes an explicit synchronous retention page separately from preview. It takes source
EX before any coordinator actor operation and retains it through unlink, parent fsync and claim completion. The
writable configured coordinator, current physical state/source directories, actual SQLite main-file identity,
exact durable run binding and sealed source-ref digest must agree. All execution generations require typed known
termination and closed mandatory collection, including recorded closed failure verdicts. An attempt whose retire
was confirmed is not waited for and has no termination to check. The retention is counted from the latest of: an
attempt ended, was retired or had its collection closed, a task was cancelled, the run was closed. A run whose
coordinator record cannot be read keeps its inputs and is counted as unknown; the page moves on to the next run
and reads that run again on the next pass. Eligibility scans the
run's generation history and is not a constant-time SQL proof. Direct task/attempt creation holds source SH through
commit and rejects an expired run; worker terminal/result processing is unchanged.

The existing persisted trusted clock policy gates every expiry, claim and physical replay. Hold never consumes
member cursors. A permanent source-run fence commits before any member release. A page examines at most 32 indexed
memberships or processes at most 32 pending claim paths. Shared, import/pending, incomplete and unproven roots stay
protected. Exact claims precede unlink and bind a publication incarnation, held store/ancestor identity, strictly
owned manifest/blob path, verified content digest and regular owned nlink=1 leaf identity. Source files 0644 and
owner-controlled 0755 nested directories are legitimate under the private 0700 source root; other permissions
cannot introduce group/world write. Every configured ancestor is opened NOFOLLOW. Only durable claimed NotFound
is idempotent, still requiring parent fsync before completion. Unknown/mismatch/permission/IO errors keep data/claim.
Completion retains the old claim and reserves a higher incarnation; completed claims never replay against a new
same-hash file. Content-addressed SourceID/hash and the permanent old-run fence are unchanged.

Source content hashing has a shared 64MiB/page budget including replay; larger/unproven legacy files are retained
as Unknown (new leaves above 32MiB cannot obtain both proof and revalidation within the page). Initial eligibility
manifest validation has a separate 64MiB cap and remains cached across member pages. Persistent root/graph/object
metadata is retained; no unbounded metadata cascade, directory removal or filesystem discovery is performed by B.
Source registration completion remains a prerequisite; the separate preview registers legacy inputs conservatively.
Only owned `manifests/src-<32lowerhex>.json` and `objects/<2lowerhex>/<64sha256>` are candidates. Git refs/objects,
bundle caches, unknown entries, originals, keys, source secret copies, run records, lock/index/root identity and
all Vault state are excluded from this source API. The D resident driver below schedules this page and the separate
C Vault page after dedicated B+C deletion review. Vault-only secret erasure must
never create a source-run expiry. Unsupported SQLite identity checks fail closed without opening/closing a live DB
validation descriptor, preserving unrelated POSIX SQLite writer locks.

Since the pruning of runs (spec 25.5) the per-run rows of this index are not permanent. The pruning of a run
deletes, under the same exclusive source lock and with the same per-file proof and claim as B, the run's root with
its memberships and graph rows, the run record and its lock, `run_expiry`, the completed `vault_delete_claim`, the
`import_session` rows and the `capture_owner` row of the run, and the bundle files of a source that no run names any
more. What the fence refused is refused by the run having no row in the parent database (the fence is only reached
for a run that has one; between the removal of the fence and of the row the mark of the run refuses a retry). The
Git refs and objects of the parent's Git area still stay (25.16).


#### Parent Vault retention (C; scheduled by D below)

`SourceStore::vault_retention_page` synchronously holds source EX then Vault EX through proof, durable claim,
unlink, directory fsync and completion. It selects at most 32 indexed runs or pending claims and shares a 64MiB
manifest/secret validation budget. Exact configured owner, immutable private run record, sealed source digest,
all-generation known terminal state, closed mandatory collection and no queued generation precede a permanent
`vault_delete_claim`. This row also fences later parent has/load/adopt/capture, returning SecretUnavailable without
recapture. Vault's separate persisted zero-delay clock has initial/anomaly Holds; every Hold blocks claims and
physical replay. Source retention and source run-expiry are independent. Unknown/incomplete legacy registration
blocks new claims. Pending exact claims survive later source-manifest removal; completed claims never replay.

A private Vault-side registration pins the canonical source root and SQLite inode. Every generic Vault constructor
and direct API honors it; missing/replaced/unreadable registrations or indexes fail closed, using SQLite's live
HAS_MOVED check without an independent validation database FD. Workers without this registration retain ephemeral
erase/reupload semantics. Direct APIs and cloneable HeldBundle retain cross-process Vault SH. Encoded secret frames
carry an opaque lease through queued Outgoing and write/flush/cut, even after caller timeout/cancellation. Writer
failure or abort drops queued frames; writer-owned buffers die before the in-flight lease. Last-lease drop explicitly
unlocks. Initial direct adoption uses atomic no-replace publication, preserving the first durable bundle.

The permanent lock file doubles as the classification pin of the registration: it is written and synced before the
marker `parent-retention.json` is published, and the marker must equal it. A first registration that died between
linking the marker and unlinking its temporary name leaves the marker with two names. With a pin, that exact shape is
read (the only other name must be the marker's own publication temporary `.parent-retention.json.<ulid>.tmp`, the same
inode) and a later sweep repairs it; nothing is unlinked on the read. Without a pin, with another second name, or with
a pin that is unsafe (not private, over 64 KiB), changed while read, for another source path, or different from the
marker, `Vault::open` is refused (fail closed) until the operator repairs the state by hand; the resident and the
command line do not start meanwhile.

Only exact private owned regular nlink=1 mode0600 run.json targets are deletable. Claims pin Vault root identity,
leaf inode/metadata and content digest; held NOFOLLOW descriptors and current anchors are rechecked before FD-relative
unlink. Durable claimed absence still requires parent fsync before completion. Changed/recreated/unsafe targets are
retained. Original workspaces, keys, Git references/caches, source secret copies and unknown entries remain excluded.
C originally exposed no automatic caller; dedicated B+C+transport review is closed and D scheduling/drain is enabled below.

Secret-free manifests never receive a Vault tombstone: their empty load stays available for source-only retry.
An unexpected Vault file for such a run is retained as Unknown. Missing bundles for manifests that require secrets
can receive a permanent exact parent tombstone, preventing later re-adoption or recapture.


#### Default resident input retention (D)

The configured parent resident enables Source/Vault retention by default using the existing `retention_days`
(default 7) for Source only. Vault has no additional age delay after all execution generations have typed known
terminal evidence, mandatory collection is closed (including valid closed failure), and no queue/retry/delivery
boundary remains. Its separate zero-retention clock still requires valid initial and subsequent native samples.
Source keeps its own persisted seven-day policy and quarantine; every clock Hold blocks claims and replay in both
protocols. A secret-bearing Source membership cannot expire before its exact completed Vault claim exists.

One owned blocking page starts immediately, then at most one every 60 seconds with missed ticks skipped. During
legacy registration the phases are registration, Vault, Source. New Source/Vault claims hold globally until persisted
legacy registration is done and no unknown pending root or incomplete graph remains; an unrelated terminal Vault run
is also held by this reviewed C precondition. Existing exact Vault claims may replay before registration completes,
but only with a valid trusted clock. Bounded legacy reconciliation remains a separate unfinished plan item. The registration phase examines at most 32 directory entries or registers at most 32 indexed
references with the existing bounded manifest cursor. Failed directory entries restart conservative discovery;
oversized/unknown/incomplete inputs remain retained. Registration touches no retention clock or generation proof.
Once registration completes the driver alternates one Vault page and one Source page, never both on one tick.
Each deletion invocation selects at most 32 runs/references or exact claimed paths, using the existing 64MiB
validation limits. No unbounded directory scan or root-sized vector is added. Generation-history eligibility still
scans the complete selected run and is not a constant-time actor operation; bounded paths/bytes do not imply bounded
CPU latency. Oversized legacy coverage and arbitrary-history latency are not claimed solved.

Only trusted platform wall/monotonic/native-boot samples enter production. Source EX and Vault EX are nonblocking;
readers, actual encoded secret sends and retry/publication guards make pages skip. Coordinator and CapturedInputs
Arcs, cursors and acquired guards remain inside owned blocking work until actual completion. Normal stop/IPC
cancellation drains accepted work before owner cleanup; no page is detached or overlapped by the loop. The driver
checks coordinator fencing before accepting work and again inside the closure, and each page checks current
physical owner/store binding. Unknown/proof/IO failures retain data and exact claims for later retry; optional GC
errors never shut down accepted user jobs. Diagnostics contain bounded counters and static descriptions, not raw
errors, paths or secret bytes. Existing result GC behavior is unchanged.

Physical deletion remains limited to the approved indexed private parent Source manifest/blob copies and exact
private parent Vault run files. Original paths, keys, Git refs/objects, caches, unknown extra files, unproven/pending
roots, run binding records, lock/index identities and worker Vault erase/reupload behavior are outside this driver.
Runtime tests invoke only private owned TempDir state; no production user-state invocation is part of validation.

# 24. グループと、runを投げたメンバー（2026-11-20）

2026-11-20の決定。親をclusterに1つ置く構成をやめ、ネットワーク上のマシンを、対等なメンバーからなる1つのグループとして扱う。親・子というマシンの種類は無い。この章が正本である。他の章の「親」「coordinator」は「そのrunを投げたメンバー（のcoordinator）」、「cluster」は「グループ」と読む。

## 24.1 モデル

| 用語 | 意味 |
| - | - |
| グループ | 互いを信頼するメンバーの集まり。`cluster_id`（グループのID）と、グループ全体で共有する1つの鍵（グループ鍵）で決まる。信頼の境界はグループである |
| メンバー | グループに属する1台のマシン（1つのOS user）。1つの `belt` バイナリと1つのresidentを持ち、residentがcoordinatorとworker agentを兼ねる。メンバーの名前はグループの中で1つで、`[[workers]]` の `id` と `worker_id` に使う |
| 投げたメンバー | あるrunを投入したメンバー。そのrunの間、変わらない。グループの中では `coordinator_id` で区別する |

- すべてのメンバーで同じ `belt` バイナリが動く。投入専用・実行専用のバイナリや導入形態は無い。
- どのメンバーも投入できる（`ci`・`exec`・`submit`）。仕事は、投げたメンバー自身を含む、空きのあるメンバーへ配る。
- runごとに正本は1つである。run・task・attempt・配置・結果は、投げたメンバーのSQLiteだけに置く。他のメンバーはそのrunの記録を持たない。合意の仕組みも分散DBも使わない（第10章）。
- 1つのメンバーにworkerは1つである。自分のcoordinatorが使うlocal workerと、他のメンバーが `worker --stdio` のbridge越しに使うworkerは、同じworker agent・同じworker DB・同じtask-traffic-controllerのscopeである。別のworkerを並べて同じhostの予算を二重に作らない。
- メンバーの一覧はユーザー設定のfile（`[[workers]]`）から読む。自動発見はしない。一覧はメンバーごとに持ち、そのメンバーが仕事を配る相手を表す。全員の一覧が一致している必要は無い。
- メンバーであることは、グループ鍵を持っていることで決まる（24.2）。一覧は「誰に配るか」、鍵は「誰の仕事を受けるか」を決める。
- 1台だけの構成は従来と同じである。`init` が、そのマシンだけのグループを作り、同じ経路を通る（第18章）。
- runは投げたメンバーで見る。`ls`・`show`・`logs`・`cancel`・`retry`・`outputs` は自分が投げたrunだけを扱い、他のメンバーのrunを一覧・操作する機能は置かない。
- coordinatorの役割は `init` を実行したメンバーが持つ。`workers add --install` で導入されただけのメンバーは、そこで `init` を実行するまで、仕事を受けるだけである（第15章）。

例：ふだん使うmain PC（`main`）と、もう1台のPC（`sub`）がある。`main` で `belt init` と `belt workers add sub --ssh me@sub.local --install` を実行し、`sub` で `belt init` と `belt workers add main --ssh me@main.local` を実行する。`sub` で `belt ci` を実行すると、`sub` がそのrunを投げたメンバーになり、jobを自分と `main` へ配る。同じ時刻に `main` で `belt ci` を実行すれば、そのrunを投げたメンバーは `main` で、2つのrunは受ける側の調停（24.4）で同じ予算を分け合う。

## 24.2 メンバーの認証、追加、削除、鍵の更新

グループ鍵は、既存のcluster鍵（`cluster_key_path` の指すfile。22.3でsecret bundleのHMACに使っているもの）をそのまま使う。メンバーごと・相手ごとの鍵は持たない。鍵には世代番号（1から）を付ける。

**認証**。メンバーであることは、双方が `group_proof` で確かめ合う。feature `group_membership_v1` は最初の公開版から必須とする（それより古い公開版は無い）。相手のSSHのOS userへ入れることと、グループ鍵を持っていることの両方が要る。

1. `hello` では、protocol versionとfeatureだけを決める。受ける側は、helloの答えに一度限りの乱数（nonce）を入れ、worker identity・boot・epoch・profile・能力はまだ返さない。
2. 接続した側は `group_proof` を送る。内容は、自分のメンバーの名前、鍵の指紋（24.2の「世代」）、自分のnonce、そして `cluster_id`・`coordinator_id`・メンバーの名前・受ける側のnonceに対するグループ鍵のHMACである。
3. 受ける側は自分の鍵で確かめる。合えば、接続した側のnonceと自分のメンバーの名前に対するHMACを答え、ここで初めてworker identity・boot・epoch・profile・能力を返す。
4. 接続した側は、その答えを自分の鍵で確かめる。合わない相手へは、仕事もsecretもsourceも送らない。

- 受ける側は、proofを確かめるまで、その接続から他の操作を受けない。`cluster_id` が違う接続、HMACが合わない接続は、理由（`group_mismatch`・`group_key_mismatch`）を付けて断る。
- **名前と `coordinator_id` の固定**: 受ける側は、proofで名乗られたメンバーの名前と `coordinator_id` の組を記録する。記録済みの名前が別の `coordinator_id` で来たとき、記録済みの `coordinator_id` が別の名前で来たときは、理由 `member_identity_mismatch` で断る。state directoryを複製・復元したマシンが別の名前で加わること、作り直したマシンが黙って同じ名前を引き継ぐことを、これで止める。組を取り替えるのは、`workers add` と `group send-key` がそのメンバーへ明示的に行ったときだけである。
  同じグループへの `workers add` もこの明示操作であり、ID・鍵・名前を渡し直し、相手では呼出元の名前と現在の `coordinator_id`、呼出元ではhelperが返す相手の名前と現在の `coordinator_id` の組を取り替えるが、service・binary・workerは追加せず、それ以外は変えない。
  `workers add --install` だけで導入した相手には `coordinator_id` を発行せず、相手で呼出元の組だけを取り替え、呼出元には相手の組を記録せず、後の `init` で発行された相手の `coordinator_id` は各メンバーへの最初の認証済み接続で通常どおり固定する。

**前提（正直なメンバー）**。グループ鍵を持つメンバーは、相手のOS userとして任意のcommandを実行できる。したがってグループは「誰の仕事を受けるか」の境界であり、メンバーどうしを互いから隔離する仕組みではない。鍵を持つメンバーが、悪意を持って別のメンバーの名前と `coordinator_id` の組を名乗ることは防がない。上の固定と24.4の「見える範囲」は、誤り（複製・復元・設定の取り違え）を防ぐためのもので、悪意のあるメンバーへの防御ではない。信頼できるマシンだけをグループに入れる。

**追加**。メンバーが `workers add <name> --ssh <dest>` を実行すると、SSH越しの固定helperが、相手へグループのID・鍵・メンバーの名前を渡す（`--install` のときはbinaryの導入に続けて行う）。鍵はSSHの接続の中だけを通し、引数やログに出さない。

- 相手が同じグループのメンバーなら、何も変えず、自分の一覧に足すだけである（名前が相手の名前と違えば断る）。
- 相手が別のグループに属するときは、何も変えずに断る。黙って移すことはしない。`--move` を明示し、かつ相手がliveなattemptも非terminalのrunも持たないときだけ、自分のグループへ移す（グループのIDと鍵を置き換える。相手の `coordinator_id` と過去のrunの記録は残る）。1つのメンバーが2つのグループに属することは無い。
- 相手が仕事を配る側にもなるには、相手の側でも `init` と `workers add` を行う（一覧はメンバーごと）。

参加する側Bからは `belt group join <既存メンバーAのSSH接続先> [--ssh <他のメンバーからBへの接続先>] [--move]` を実行できる。Bは `init` 済みであること（未導入・workerのみなら先に `belt init` を求めて断る）。名前は `init` が記録したものを使い、joinに `--name` は置かない。`--ssh` の既定値は `<現在のOS user>@<hostname>`。まずBがSSHでAの固定helperへBの名前・接続先・move指定をstdinの小さなJSONとして渡す。Aは既存の `workers add`（`--install` 無し）でBを追加し、BのグループID・鍵と双方のpinを更新する。AからBへ `--ssh` で到達できなければ、その必要を示して断る。次にBはhelperが返したAの名前と最初のSSH接続先を使い、同じグループへの `workers add` でAを自分の一覧に追加する。Bの旧グループが自分だけ（他の名前の一覧行もpinも無い）で仕事が無ければ `--move` 無しで置き換える。それ以外の移動には既存の `--move` の規則を使い、liveな仕事・未回収の結果があれば移さない。同じグループへの再実行はpinを取り替える既存の追加処理と同じである。第1段だけ成功したときは「BはAのグループに入ったが、BでのAの登録が完了していない」と報告し、巻き戻さない。再実行で第2段まで完了できる。

`belt init [--name <メンバー名>]` の名前の既定値は短いhostname（最初の `.` より前）を `[A-Za-z0-9._-]` 以外の文字を `-` に置き換えたものとする。1〜128文字、先頭の `.` と `..` を禁止する名前の形式に合わなければ、`init --name` の指定を求めて断る。workerのみのホストの引継ぎでも初期化済みホストでの再実行でも、`--name` は省略または既存の名前と同じ値だけを許し、renameは行わない。同名のホストが出会ったときは既存のpinによる `member_identity_mismatch` で断り、新しいホストの初期化時に `init --name` で別名を選ぶよう案内する。

**削除と鍵の更新**。メンバーを外すには、残るメンバーの一覧からそのメンバーを消し、鍵を更新する。beltの中の信頼を取り消すのは鍵の更新であり、一覧から消すだけでは、外したメンバーからの仕事を断れない。メンバーごとの登録・取消の操作は置かない。

鍵の更新は、SSHのアクセスを取り消さない。外したマシンのSSH鍵が残るメンバーの `authorized_keys` に残っていれば、そのマシンはbeltを通さずに、同じOS userとしてcommandを実行できる。beltは `authorized_keys` を編集しない。`group rotate-key --remove` は、残る各メンバーのbelt用accountから外したマシンのSSH鍵を消す手順を表示する。`group` と `doctor` は、外したメンバーへSSHでまだ到達できる間、警告を出し続ける（到達できなくなったら消える）。一覧の行を消した後も、警告が消えるまで、名前と削除時刻を小さな「削除済みメンバー」の記録に残す。

鍵の「世代」は、番号と鍵の指紋（鍵から導く短いdigest。鍵そのものは復元できない）の組で表す。どの鍵を持っているかは指紋で比べ、番号だけで判断しない。

```sh
belt group join me@main.local --ssh me@sub.local # 参加する側から双方の一覧へ追加
belt group rotate-key --remove old-mini   # 一覧から消し、残る全員へ新しい鍵を配って切り替える
belt group send-key mini-b                # 取り残されたメンバーへ、今の鍵を送り直す
belt group                                # グループのID、鍵の世代、メンバーごとの状態
```

`group rotate-key` は、残るメンバーのどれか1台で実行する。そのメンバーの一覧（`[[workers]]`）に、残る全員が載っていること。手順は2段で、どこで止まっても同じcommandの再実行が続きを行う。

1. **配る**: まず、一覧の各メンバーから、そのメンバーの一覧（`[[workers]]`）を固定helperで集める。集めた一覧の和集合に、自分の一覧にも `--remove` にも無いメンバーが居れば、何も配らずに断り、そのメンバーを示す（知らないメンバーを黙って取り残さないため）。次に、既に次の鍵を持っていればその鍵を使い、無ければ新しい世代の鍵を作り、一覧の各メンバーへ渡す。受け取ったメンバーは、それを「次の鍵」として保存する。この段では、全員が今の鍵で署名し、今の鍵と次の鍵の両方を受け入れる。動いている仕事には何も起きない。1台でも渡せなければ、ここで止まり、渡せなかったメンバーを示す（`--skip <member>` を付けたメンバーは後回しにできる）。一覧の収集でも `--remove` と `--skip` の対象へは接続せず、和集合に現れる実行ホスト自身は既知のメンバーとして扱う。
2. **切り替える**: 全員（skipを除く）へ渡せたら、各メンバーへ切替を指示する。切り替えたメンバーは、次の鍵を今の鍵にし、古い鍵を捨て、メンバー間の接続をすべて閉じる。以後は新しい鍵のproofだけを受け入れる。残るメンバーどうしはすぐにつなぎ直し、attemptはleaseの範囲で続く（短い切断と同じ。第12章）。

秘密情報のcaptureにはその時点のグループ鍵を使い、常駐プロセスも切替後に古い鍵を使い続けない。切替前にcaptureしたsecret bundleを切替後に配置するrunは、古い鍵が破棄されて検証できないため拒否され、再投入が必要になる。

- **自分で切り替える**: 次の鍵を持つメンバーが、今の鍵のproofを `group_key_mismatch` で断られ、相手の示した指紋が自分の次の鍵のものだったときは、切替の指示を待たずに自分で次の鍵へ切り替える。切替の指示が届かなかったメンバーは、これで追い付く。相手が認証前に名乗った指紋で切り替えることは、上の正直なメンバーの前提で許容する。
- **同時の更新**: `group rotate-key` を実行したメンバーが既に「次の鍵」を持っていれば、その鍵（同じ指紋）を使い、止まったところから更新を続ける。配る段で、相手が自分の知らない別の指紋の「次の鍵」を持っていたら、何も上書きせずに断り、先の更新を続けるよう示す。`--remove` を付けたときは、外すメンバーが次の鍵を知っているかもしれないので、既にある次の鍵は、その更新を始めたときに同じメンバーを外していた場合だけ使う（更新の記録に、始めたときに外したメンバーを残す）。再実行では `--remove` の省略や一部だけの指定でも、記録に残る全メンバーを配布対象から外し、登録の削除を完了する。それ以外で次の鍵か更新の記録があれば、集める前に断り、先に `--remove` なしの `group rotate-key` で前の更新を終えてから、もう一度 `--remove` を付けて実行するよう示す。
- `--remove <name>` は、実行したメンバーの一覧からそのメンバーを消し、鍵を渡さない。他のメンバーの一覧に残っている行は、そのメンバーの `doctor` と `group` が `group_key_mismatch` として示すので、それぞれで消す。
- 次の鍵を受け取っていないまま取り残されたメンバー（skipしたもの）は、古い鍵のままなので断られる。残るメンバーの1台から `group send-key <member>` で今の鍵を渡すと戻る。止まっていた間にleaseが切れたattemptは、通常の規則どおりworkerが止めて `interrupted` と答える。
- 2〜5台を想定し、渡す順は一覧の順、同時には行わない。鍵の更新の間、新しい投入と配置は止めない。
- **鍵の切り替えの後始末**: 管理されたservice（`belt init` が登録したもの）では、鍵を切り替える前に、新しい鍵のproofを `init.json` の `switching_key` に記録し、切り替えのrenameのあとで消す。renameの前後のどこで止まっても、読み手は記録した2つのinodeのどちらか一方だけを受け入れる。止まったまま残った `switching_key` は、daemonの起動時、`upgrade` の開始時、`init` で、`init.json` から消して書き戻す。これで、`upgrade` の巻き戻しで古いbinaryが `init.json` を読める。切り替えの途中の別のprocessが `init.lock` を持っているときは、起動時の後始末はそのprocessに任せて何もしない。
- **workerが受け取る新しい鍵**: workerは、新しい鍵を `<key>.installing` に置いてから切り替える。置くときは新規作成のみ（既にあるfileやsymlinkを開かない）、mode 0600、symlinkを辿らない。前回の途中で止まって残った `<key>.installing` は、symlinkのときはリンクとして外し、リンク先には触れず、新しく作り直す。

**外したメンバーに関わる仕事**。

| 仕事 | 扱い |
| - | - |
| 外したメンバーが投げたrunのattemptで、残るメンバーの上にあるもの | 切替で接続が閉じ、外したメンバーはつなぎ直せないので、renewが来ない。残るメンバーのworkerは、leaseの失効で止めて `interrupted` に確定する（既存の規則）。未回収の結果は回収されないまま、保持期間で消える |
| 残るメンバーが投げたrunのattemptで、外したメンバーの上にあるもの | 投げたメンバーからは到達不能と同じに見え、unknownのまま保持する。時間の経過では置き直さない。利用者が `workers forget <name>` を実行すると、`interrupted`（cancel済みは `cancelled`）に確定し、回収を断念として閉じる（第15章と同じ確定の規則）。再実行は明示retryかretry policyによる。未回収の結果は失われる |

- `workers forget` は、一時workerのほかに、「最後に接続した後でグループ鍵が更新され、今接続していない」メンバーにも使えるものとする。ただしこの場合は、そのメンバーと最後に連絡が取れた時点から、leaseの1期間と停止猶予（`lease_seconds` とTERM後の猶予。第12章）が過ぎるまでは確定しない。過ぎる前の `forget` は、残り時間を示して断る。相手のworkerがleaseの失効で止め終える時間を待つことで、確定の後のretryが、止まっていないcommandと重ならないようにする（正直なメンバーの前提）。`group rotate-key --remove <name>` は、実行したメンバーについて、この時間が過ぎた後に `workers forget` を実行するよう案内する。他のメンバーも、そのメンバーにattemptを置いていた場合に、自分で実行する。
- **forgetしたメンバーを戻すとき**: `group send-key` で常駐のメンバーを戻すと、そのメンバーのworkerは新しいworker identityで接続する。仕組みは一時worker（第15章）と同じで、workerは `<OSのboot id>.<ULID>` の後半を保存しておき、`send-key` を受け取ったときに作り直す。作り直す前に、古いidentityで受けていたliveなattemptを止めて `interrupted` に確定する。`send-key` はforgetの有無によらず毎回identityを作り直すので、forgetしていなくてもliveなattemptが止まるのは意図した動作である。forgetした側には古いidentityのtombstoneが残るので、戻ったworkerは「新しいidentity」として通り、古いidentityのattemptが新しい接続へ送られることは無い。
- 外したメンバーの側では、自分のrunのattemptはunknownのまま残る（残るメンバーが `group_key_mismatch` で断るため）。グループへ戻す（`send-key`）か、そのメンバーのstateを作り直すまで解消しない。初回リリースではこの扱いとする。
- 外したメンバーのworkerは、renewが来ないので、受けていたattemptを自分のleaseの規則で止める（正直なメンバーの前提）。

親DBやstate directoryのcopyを別のマシンで起動する構成は、第18章のとおりサポートしない。copyが別の名前で加わろうとした場合と、同じ名前で別の `coordinator_id` が来た場合は、上の「名前と `coordinator_id` の固定」で断る。同じ名前と同じ `coordinator_id` のcopyが同時に動く場合だけは区別できない。

## 24.3 workerのidentityと、退役の通知

- workerのidentityとそのtombstone（第15章）は、それを記録したメンバーのDBの中だけのものである。あるメンバーがworkerのidentityを退役させても、他のメンバーの記録は変わらない。`workers forget` と「同じ名前で新しいidentityが接続した」ことによる退役が確定するのも、そのメンバーが投げたattemptだけである。
- 一時workerの起動ごとのidentityは、グループの全員に共通の1つである。起動時にグループのIDと鍵を受け取る（`--cluster`・`--name`・`--key-file`。従来どおり）ので、鍵を持つどのメンバーも使える。

**退役の通知**（wire）。退役済みのidentityがhelloに答えたとき、投げたメンバーはattachしない（従来どおり）。投げたメンバーは、接続を閉じる前に `identity_retired`（`coordinator_id`、`worker_id`、退役させた `worker_boot_id`）を1回送る（feature `identity_retired_v1`。最初の公開版から必須）。

- workerは、`worker_id` が自分の名前で、`worker_boot_id` が自分の今のidentityのときだけ受け付ける。それ以外は何もせずに断る。
- 受け付けたworkerは、そのメンバーが投げたliveなattemptを、leaseの失効を待たずにcancelと同じ手順で止めて `interrupted` に確定し、「このメンバーからこのidentityの退役を告げられた」ことを永続化する。以後、同じidentityである間は、そのメンバーの接続を理由 `identity_retired` で断る。
- 他のメンバーが投げたattempt・接続・leaseには触れない。
- 一時workerは、退役を告げられた後、どのメンバーのliveなattemptも無く、そのidentityをまだ使っているメンバー（退役を告げていないメンバー）の未回収のresultも無くなったら、stateを消して終了する。それまでは他のメンバーの要求に答え続ける（退役を告げたメンバーの接続は `identity_retired` で断り、そのメンバーのresultは回収されないので数えない）。
- 通知は同じ内容の再送に同じ答えを返す。届かなくても安全性は変わらない（submitもrenewも送られないので、attemptはleaseの失効で止まる）。通知の目的は、失効を待たずに止めて資源を空けることである。送った側は送ったことを記録するが、届いたことを前提にしない。

## 24.4 投げたメンバーをまたぐ調停

取り合いは、受ける側（worker）で調停する。投げたメンバーは、他に投げているメンバーがいることを前提にしない。

- **予算は1つ**: workerの受付窓（`max_active_task + prefetch_per_worker`。第9章）と、task-traffic-controllerのadmission（第16章）は、すべてのメンバーが投げた仕事を合わせた1つの予算である。メンバーごとの枠は持たない。
- **窓の出所**: 受付窓は、受ける側のメンバー自身のユーザー設定（そのマシンの `[coordinator] max_active_task` と `[scheduler] prefetch_per_worker`）で決まる。workerは `inspect` でこの値を答える。投げたメンバーは、自分の設定の値とworkerが答えた値の、項目ごとに小さい方で数える（第9章）。投げたメンバーの設定が、他のマシンの窓を広げることは無い。
- **workerが正**: workerは、terminalでないattemptをすべてのメンバーの分について数え、受付窓を超える新規submitを、何も保存せずに断る。投げた側が自分の配置を数えて上限で止める規則は残すが、断られるsubmitを減らすための最適化である。受理済みattemptの再送は数えず、断らない（第11章の冪等性）。
- **断り方**: `OVERLOADED` に `overloaded_reason.reason = "capacity"` を付けて答える（feature `shared_admission_v1`。最初の公開版から必須）。知らない `reason` は理由なしとして読む。断ったworkerは何も保存しない。tombstoneも残さない。
- **断られた側**: `capacity` の答えは「workerは何も保存していない」という確定した答えである。そのattemptを1回だけ送り、その1回に `capacity` が返ったときは、届いていないcopyはどこにも無いので、retireを送らずにそのattemptを閉じ（理由 `capacity_refused` を自分のDBに記録する。wireには載せない）、次のgenerationで別のworkerへ配置する。送信回数は、送る直前に親DBへ記録する（答えの無い送信も1回に数える）。`in-place` のtaskは自動で配置し直さない（第6章）ので、この断りを理由の無い `OVERLOADED` と同じに扱い、同じattemptを同じworkerへ後で送り直す。
- **結果が不確かなとき**: そのattemptを2回以上送っていた、または答えの無い送信が1回でもあった場合は、遅れて届くcopyが残り得る。coordinatorの再起動後、送信回数が永続記録から分からないattemptも、結果が不確かなものとしてretireを通す。このときは従来どおりretireを依頼し、workerのtombstoneと未開始の確認を得てからtaskを空ける（第11章の「retireしてから置き直す」規則）。遅れて届いた元のsubmitは `RETIRED` で断られ、同じ仕事が2か所で走らない（A19）。確認が取れない間はunknownのまま保持し、置き直さない。
- **除外は一時的**: `capacity_refused` のworkerは、`worker_cannot_run`（第11章）と違い、そのtaskから恒久的には外さない。そのworkerが次の `inspect` で受付窓の空きを答えるまで、どのtaskもそこへ新しく配置しない。空きを答えたら候補に戻す。この状態はprocessの中だけに持ち、再起動後は `inspect` の答えから決め直す。`inspect` が満杯を答えたworkerへは、断られる前でも配置しない。その判定には、`inspect` の答えた他のメンバーの分と、投げた側が自分でそのworkerに置いている件数の合計が受付窓に達しているかを使う（断られるsubmitを減らすための最適化）。
- **置き直す先が無いとき**: taskは未配置のまま待ち、理由（`capacity`）を `task_waiting` に残す（`blocked_reason` が `capacity`、外したworkerの理由も `capacity`）。taskを失敗にしない。
- **`inspect`**: 受付窓の上限と使用数、そのうち聞いたメンバーの分と他のメンバーの分（件数だけ）、そのマシンが自分で投げた非terminalのrunを持つかどうかを答える（最後の項目は、それを読む `workers stop` と一緒に足す）。
- **公平さ**: 受付は到着順である。メンバーごとの予約や比例配分は初回リリースに含めない。1つのメンバーが窓を埋めている間、他のメンバーはそのworkerを使えないが、失敗にはならず、他のworkerへ置くか待つ。自分のlocal workerにも優先枠は無い。自分のマシンの窓が他のメンバーの仕事で埋まっていれば、自分のrunも、他のworkerへ置くか待つことになる。
- **その他の共有上限**: `max_live_attempts`、同時に行うworkspaceの準備、blobの受け取りの量と個数、覚える `sync_id` の数（第11章）も、すべてのメンバーの合計に対する上限である。超えた要求は従来どおり理由なしの `OVERLOADED`（何も行っていないので送り直せる）で断る。
- **runの中の制約**: matrixのglobal permit、ShardGroup、fail-fast、joinは、runの中の制約なので、投げたメンバーが管理する（第12章）。
- **epoch**: `coordinator_epoch` は投げたメンバーごとの世代である。workerは `coordinator_id` ごとに最大のepochを持ち、`STALE_EPOCH` の判定、lease key（`coordinator_epoch`・`lease_seq`）、helloの答えの `coordinator_epoch` には、そのメンバーのものだけを使う。あるメンバーのepochが進んでも、他のメンバーの要求は古くならない。「何も行っていない」ことを表す断り（`OVERLOADED`、`INVALID_INPUT`、taskにliveなattemptがあることによる `CONFLICT`）は、その要求が運んだ新しいepochも記録せず、epochが進んだときのtombstoneの消去も行わない。attemptについての答え（`RETIRED`、payloadの不一致、`LEASE_NOT_RENEWABLE` など）は、従来どおり新しいepochを記録する。
- **所有と見える範囲**: attemptは、それをsubmitしたメンバー（`coordinator_id`）のものである。workerは、attempt・retireのtombstone・leaseを `(coordinator_id, attempt_id)` で、taskごとのliveなattemptの一意性を `(coordinator_id, task_id)` で持つ。したがって、別のメンバーが同じ `attempt_id` や `task_id` を使っても別の行になり、互いに `RETIRED` や `CONFLICT` を起こさず、相手の状態も返らない。`get_attempt`・`list_attempts`・`renew_lease`・`cancel_attempt`・`retire_attempt`・`watch_events`・`read_log`・`get_result`・`ack_collected`・`blob_get`・`secret_bundle_put` は、接続したメンバーの分だけを対象にする（他のメンバーのattemptは `NOT_FOUND`）。
- **`attempt_id` は1つのworkerで1メンバーの分だけ**: worker上のattemptのdirectory、supervisor、task-traffic-controllerのcommand IDは、`attempt_id` だけで名前を付ける。そのため、1つのworkerが同じ `attempt_id` のattemptを持てるのは、同時に1つのメンバーの分だけである。別のメンバーのattemptが持っている `attempt_id` へのsubmitは、tombstoneを含めて何も保存せずに、`OVERLOADED` と `overloaded_reason.reason = "attempt_id_in_use"` で断る（相手のattemptの状態は答えに含めない）。断られた側は、taskを失敗にせず、新しい `attempt_id` で配置し直す（`in-place` のtaskは同じattemptを後で送り直す。第11章）。tombstone・lease・epochはこの制限を受けず、同じ `attempt_id` へのcancelやretireは、送ったメンバー自身のtombstoneになるだけで、相手のattemptには触れない。`attempt_id` は乱数で作るので、正直なメンバーの間でこの衝突は実際には起きない。
- **届かなかったattemptのtombstone**: retireやcancelが先に届き、attemptが届かなかったtombstoneは、そのメンバーの `coordinator_epoch` が記録時より進んだら消してよい（古いepochの遅れたsubmitは `STALE_EPOCH` で断られる）。メンバーごとに1万件を上限とし、超える依頼は `OVERLOADED` で断る。受理したattemptのtombstoneは、attemptと一緒に消え、重複防止の記録が置き換わる（25.6）。
- **drainとstop**: `workers drain`・`resume` はメンバー全体に働く（どのメンバーからの新しいsubmitも止める）。状態はworkerの側に永続化し、どのメンバーからでも `resume` できる。更新のための保留（24.8）はdrainとは別の状態で、`resume` では解けない。`workers stop` はそのマシンのresident（両方の役割）を止めるので、相手が他のメンバーの仕事を持つか、自分で投げた非terminalのrunを持つとき（`inspect` の答えで分かる）は、理由を示して断る。`--all-submitters` を明示したときだけ進める。`stop --cancel-running` も同じ条件で `--all-submitters` を要する。

## 24.5 投げたメンバーをまたぐ予測と見積

- workerのforecastは、実ledger（誰が投げたかを問わず全部）、workerが受理した未登録のworkload（すべてのメンバーの分）、聞いたメンバーが送信中の未ACKの仕事（そのメンバーが渡す）を含める。
- 他のメンバーが送信中でまだworkerに届いていない仕事は、どこからも見えない。forecastは予約券ではない（第9章）ので、その分は実際のsubmitの時点の断り（24.4）で吸収する。
- coverageとnodeの識別子（第9章）は、聞いたメンバーの分だけを返す。他のメンバーの需要は合成済みの結果に効かせ、件数（他のメンバーの非terminalのattemptの数）だけを `foreign_demand` として返す。識別子・command・sourceは返さない。聞いた側は、合成済みの需要をoverlayへ足し直さない。
- 配置理由と `estimate` の表示には、他のメンバーの需要を含むことと、その件数を出す。`estimate` は読み取り専用のままで、他のメンバーの仕事があっても何も増やさない（A06）。
- 自分のlocal workerも同じ扱いである。他のメンバーの仕事が入っていれば、自分のrunのforecastにその分が現れる。

## 24.6 障害の範囲

| 障害 | 止まるもの | 続くもの |
| - | - | - |
| あるメンバーのresidentが落ちる | そのメンバーが投げたrunの進行（配置・lease更新・回収）。そのメンバーのworker agent | そのrunのattemptは、置かれたworkerでleaseの範囲だけ続き、切れたら止まる（第12章）。supervisorは別processなので、そのマシン上のattemptもleaseの範囲で続く。他のメンバーが投げたrunは、残りのworkerの上で続く |
| あるメンバーのマシンが落ちる（電源・OS） | 上に加えて、そのマシン上で動いていたすべてのattempt | 他のメンバーから見ると「workerの到達不能」「worker OS再起動」（第12章）で、それぞれが自分のattemptをその規則で扱う |
| residentが戻る | — | coordinatorは自分のrunの非terminalをreconcileして同じattemptをadoptし、worker agentは生きているsupervisorへ再接続する（第12章の2つの行が、1つのprocessで両方起こる） |

- workerは投げたメンバーごとにleaseを見る。あるメンバーが落ちてleaseが切れても、止めるのはそのメンバーのattemptだけである。
- 投げたメンバーが落ちたrunを、別のメンバーが引き継ぐことはしない（第2章の対象外）。そのresidentが戻るまで、そのrunは進まない。
- 投入は、自分のresidentが動いているときだけできる。他のメンバーに投げさせる機能は置かない。

## 24.7 遅いpeerが他を止めない（R96）

2026-11-20時点の実装では、coordinatorのtickが応答しないworkerと話している間、1台あたりの上限 `worker_budget_ms`（既定15秒）のあいだcoordinatorのactorを持ち続け、その間のCLIの要求が時間切れになる。メンバーが増えるとこの時間は重なって増えるので、次を必須とする。

1. **actor**: 配置のcommitとDBの書き込みを直列化するactorは、他のメンバーとの通信を待つ間、占有されない。通信はworkerごとの独立した処理で行い、actorへは結果だけを渡す。
2. **CLI**: 自分のDBだけで答えられる要求（投入の受理、cancel・retryの受理、`ls`、`show`、回収済みの `logs`・`outputs`、`workers`・`group` の一覧、`workers forget`）の応答時間は、メンバーの台数と状態に依存しない。到達不能・無応答のメンバーが何台あっても同じである。
3. **照会が要るCLI**: `estimate`、`workers diff`、`doctor`、未回収分の `logs --follow` などは、答えたメンバーの分を、答えないメンバーを待たずに（メンバーごとの上限で）返し、答えなかったメンバーを明示する。
4. **worker どうし**: 1台の遅いworkerが、他のworkerのlease更新・確認を遅らせないという規則（第11章）を、配置のsubmit、回収、`env_sync`、blobの転送にも広げる。
5. **受ける側**: あるメンバーの接続が遅い・止まっている・大きな転送の途中であっても、他のメンバーのhello・`inspect`・`forecast`・submit・renew・cancelを塞がない。メンバーをまたいで共有するlockを、通信を待つ間持ち続けない。
6. **同じprocessの中**: 1つのresidentが両方の役割を持つので、自分のcoordinatorが遅いメンバーを待っていても、同じマシンのworker agentが他のメンバーへ答える時間は変わらない。逆も同じである。

受入（A56）では、接続は保ったまま答えないメンバーを置いた状態で、2の要求が2秒以内に返ることを確かめる。この2秒は実装の定数とし、`worker_budget_ms` より十分短くする。試験は仮想の時計で測る決定的なものとし、sleepに頼らない。

## 24.8 メンバーの更新（R68: residentが自分を更新する）

residentが自分自身を更新する機能（R68）は、初回リリースの範囲に含める。どのメンバーも投げる側なので、すべてのメンバーのresidentを更新する手段が要る。`workers upgrade` は、動いているresidentが自分のbinaryと同じbuildのときだけ進む（第18章）ので、まず自分を更新できなければ、他のメンバーも更新できない。

- `belt upgrade [--binary <path>]` は、自分のマシン（1つのresident、両方の役割）を更新する。更新のための保留を置き、受理済みの仕事の完了を待ち、親DBとworker DBを退避し、新しいbinaryを版別directoryへ置き、互換を検査し、serviceを切り替え、healthを確かめ、commitして保留を解く。実行中のbinaryを上書きしない。
- **更新のための保留**: 更新の開始時にworkerへ置く、drainとは別の永続状態である。新しいsubmitを受けない点はdrainと同じだが、`workers resume` では解けず、解くのはcommitかrollbackだけである。利用者が先にdrainしていた場合、commitの後もそのdrainは残る。
- **試用中は誰にも約束しない**: 切り替えた新しい版は、commitされるまで試用中である。試用中のworkerは、他のメンバーからの状態を変える要求（submit・renew・retire・cancel・`ack_collected`・blobやsecretの受け取り・`env_sync`・`identity_retired`）のすべてに、何も行わず、何も保存せずに、後で送り直せる答え（理由なしの `OVERLOADED`）を返す。読み取り（`inspect`・`get_attempt`・`forecast`）には答える。したがって、他のメンバーのretireやcancelは、相手の側で未確認のまま残り、commitかrollbackの後に届け直される。試用中に受け取ったtombstoneや確認が、rollbackで消えるということが起こらない。保留で受理済みの仕事の完了を待ってから切り替えるので、試用中にrenewを断られて止まるattemptは無い。
- **試用中のcoordinator**: coordinatorの役割は、既存のattemptのadoptとlease更新だけを行う。新しい配置、新しいgenerationの作成、投入の受理、retire・cancelの送信、回収の確定（`ack_collected` と結果の採用）は行わない。
- **退避とrollback**: 切替の前に、親DB（自分が投げたrun）とworker DBの両方を、同じ時点で退避する。新しい版のmigration（coordinatorとworkerのstep）は古い版から読めないので、rollbackは2つのDBを一緒に退避へ戻す。片方だけを戻さない。commitの後は戻せない（第18章のworkerの規則と同じ）。
- **誰が決めるか**: 外側から見ている別のメンバーは居ないので、`belt upgrade` を実行したCLIが、新しい版のhealth（`hello` とfeature、両方のDBが開けること、adoptしたattemptのleaseを更新できたこと）を確かめてcommitを指示する。
- **期限**: commitには期限（実装の定数。leaseの1期間より短くする）を置く。期限を見張るのは、serviceが起動する入口（版を選んでresidentを起動する部分。切替の前から動いている、更新されない側）である。期限までにcommitされなければ、新しい版が応答していなくても、入口が新しい版を止め、2つのDBを戻し、元の版を起動する。新しい版の起動時の確認だけに頼らない。CLIが途中で落ちた場合も同じ期限で戻る。
- **rollback後に二重実行しない**: 試用中は、自分では新しい配置もgenerationも作らず、他のメンバーへは何も約束していない。lease更新は同じlease keyの続きとして行う。したがって、戻した後の古い版は、退避した時点の記録から同じattemptをadoptするだけになり、他のメンバーが試用中に出したretireは、戻した後に届いてtombstoneになる。試用中に進んだ `lease_seq` は、戻した版がworkerの答えから読み直す。
- 自分が投げたrunは、切替の間、process再起動と同じ扱いになる（第12章）。他のworker上のattemptはleaseの範囲で続く。切替がleaseより長引いた場合は、worker側の既存の規則で止まる。
- `workers upgrade <worker>` は、SSH先のマシンで同じ手順を起こす。更新されるのはそのマシンの1つのbinaryで、相手が投げたrunも上の規則で保たれる。このときは、実行した側のCLIがhealthを確かめてcommitする。期限を見張るのは、相手のマシンのserviceの入口である。
- 同じマシンに対する更新は、process lockで同時に1つだけにする。
- 版の間の互換は第18章のN／N+1のままである。メンバーはどの順に更新してもよく、NのメンバーとN+1のメンバーは、投げる側・受ける側のどちらの向きでも互いを使える。

## 24.9 採用しなかった案

案A（Raftなどの合意で、すべてのメンバーが同じ状態を持つ構成）は採用しない。

- 2台では過半数が成り立たない。最も多い構成が2台で、1台が落ちると全体が止まる。
- coordinator・storage・復旧の規則を大きく書き直すことになる。
- secret（GitHub status用のcredential、secret bundle）を全メンバーへ複製する必要がある。
- 古いleaderの書き込みを止めるfencingが要る。「世代を増やしただけで古いプロセスが停止したとは扱わない」（第10章）という原則と両立させる仕組みを別に作ることになる。

この章の構成は、投げたメンバーが落ちたrunが止まることを受け入れる代わりに、これらを要らなくする。対等なのは「誰でも投げられ、誰でも受ける」ことであり、runの記録を全員で持つことではない。

投げるメンバーごとにworkerが信頼を登録・取消する構成（メンバーごとの鍵と取消の記録）も採用しない。2〜5台では、グループ鍵1つと鍵の更新で足り、登録・取消・相手ごとのdrainの操作と記録を持たずに済む。

## 24.10 wireと移行のまとめ

| feature（hello。どれも最初の公開版から必須） | 内容 |
| - | - |
| `group_membership_v1` | 操作 `group_proof`（相互）、helloの答えのnonce、断る理由（`group_mismatch`・`group_key_mismatch`・`member_identity_mismatch`・`identity_retired`） |
| `identity_retired_v1` | 操作 `identity_retired`（24.3） |
| `shared_admission_v1` | submitの `overloaded_reason.reason = "capacity"`、`inspect` の受付窓の使用数と共有の有無、forecastの `foreign_demand` |

- 3つとも必須にする。これより古い公開版は無いので、持たない相手との互換の枝は置かない。持たない相手とは接続しない。schema versionは上げない。
- helloの要求は従来から `cluster_id`・`coordinator_id`・`coordinator_epoch` を運ぶので、項目は増やさない。helloの答えは、nonceを加え、worker identity・boot・epoch・profile・能力を `group_proof` の答えへ移す。
- 鍵の配布・切替・送り直し（`workers add`、`group rotate-key`、`group send-key`）は、SSH越しの固定helperで行い、worker protocolの操作にはしない。
- 既存のworker DBの移行（step 4）: 最初の公開版より前のDBなので、誰のattemptだったかを推測して引き継ぐことはしない。移行は、workerがdrain済みでliveなattemptが無いときだけ行い、liveなattemptがあれば、その件数と、drainして完了を待つ手順を示して断る（DBには触れない）。移行では、古いattempt・tombstone・lease・epochの行を捨てる。古いcoordinatorも同じ更新で新しい版になり、新しい鍵の形（`(coordinator_id, …)`）の下でepochを数え直す。古い版の遅れたsubmitは、`group_proof` を通れないので届かない。今の `cluster_id` と鍵は、そのままグループのIDと世代1の鍵にする。

# 25. ディスクに残すものと消す時点（2026-11-23）

beltがディスクに書き、書いたcommandの終了後も残るものを、この章ですべて挙げ、消す規則を定める。

この章は実装に先行し、**段階**（25.2）ごとに実装する。各段階は、それだけで実装して出せる。ある段階が実装されるまでは、他の章の現在の記述が実装の挙動であり、この章の規則は「その段階が入ったときにこうなる」という予定である。段階が変える他の章の文は25.14に挙げる。25.10の表は、行ごとに「今の挙動」と「その行を成り立たせる段階」を分けて書く。25.9は、段階5bで閉じなかったことを置く。

## 25.1 原則

1. **読み手のないものは書かない。** 残すものには、それをもう一度読む処理（読み手）が必ずある。読み手が無いものは書かないか、書いたcommandの中で消す。
2. **量と期間は読み手が使う分だけにする。** 読み手が使う分より大きい上限、長い期間は誤りとする。上限の数値には、その値を決めた読み手を書く。
3. **要らなくなった時点で消す。** 消すのは、要らなくなった出来事（runの刈り取り、結果の引き渡し、更新のcommit、参照が無くなったこと）を処理するその場である。定期の走査と起動時の走査は、crashで取りこぼした分の受け皿としてだけ置き、契機と費用の上限を25.11に定める。
4. **持ち主と一緒に消す。** marker、空のdirectory、stamp fileは、持ち主（run、attempt、source、版）が消えるときに一緒に消す。lock fileは下の規則に従う。
5. **使われていない証明が無ければ消さない。** 各規則に、誰がまだ読める・lockできる・書けるか、なぜそれが居ないと言えるかを書く。証明できないものは残し、`doctor` が件数を示す。beltが書いたと証明できないfileには触れない。
6. **消す処理は安い。** 索引か直接のpathで答えが出るものを、commandごとの全走査で探さない。1回の処理が扱う件数とbytesに上限を置く。
7. **時刻で消すものは、信頼できる時計でだけ消す。** 保持期間の判定は、結果のGCと同じ時計の規則（第23章）に従う。時計がHoldの間は、期間で消すものを何も消さない。出来事で消すものは時計に依らない。
8. **更新の試用中は消さない。** `upgrade` の試用中（24.8）は、rollbackが親DBとworker DBを退避へ戻す。戻した行が、消したfileを指してはならない。この章の消す処理は、試用中はすべて止める。試用中のcoordinatorは `release_attempt`（25.6）も送らない。

**lock fileの規則。** flockに使うpathをunlinkしてよいのは、次のどちらかを示せるときだけである。どちらも示せないlock fileは**固定のfile**として25.10に挙げ、unlinkしない。古い版のprocessには、PATHに残った古いCLI、更新をまたいで動き続けるsetup builder、更新の番人（upgrade guard。前の版のbinaryで動く）を含める。「commitの後に古い版は動いていない」は、residentとsupervisorについてしか言えない。

- **(A) そのpathを開くprocessが、どの版のものも残っていない。** pathを開く経路をすべて挙げ、各経路について、開く理由がもう無いこと（持ち主の行が無い、持ち主のIDは使い回さない、名指す台帳のentryが無い、開く側のprocessが居ないことをlockで確かめた）を示す。
- **(B) そのlockを持った最初の版から、取得側の全員が、lockの後にdescriptorのinodeとpathのinodeの一致を確かめ、確かめるだけの側はfileを作らない。** この場合、古い版のprocessが消えたinodeを開いても、照合で「無い」になる。

| lock | 開く経路 | 根拠 | 扱い |
| - | - | - | - |
| `reader-pins/<run>.lock` | resident（作成とGC）と、CLIの `logs`・`outputs`。pinを開く処理（`RunPin::ensure`・`shared`・`exclusive`）は、すべて親DBを開いた後の `CoordinatorStore` の中にある | (B)。`src/results/pin.rs` の `checked` が最初からinodeを照合し、readerは作らない。作る側（3つ。25.5）は、runの行があって印が無いときだけ作る | runと一緒に消す（段階2） |
| `sources/runs/<run>.lock` | そのrunを取り込むprocessだけ（`SourceStore::lock_run`。run IDは取り込むprocessが作る） | (A)。lockを作る前に持ち主の行（`capture_owner`）があり、runの行ができた後に、このlockを開く経路は無い。提出前は持ち主のprocessが生きていないことを示す（25.4） | runと一緒に消す（段階1・段階2） |
| `supervisors/<attempt>.lock` | そのattemptのsupervisorの起動、agent（5か所）、別のscopeで動くsupervisorの照合（`config::traffic::foreign_supervisors`。`attempts/<attempt>/supervise.json` がある間）。一覧は25.6 | (A)。条件は25.6に書く。`SupervisorLock::try_acquire` はfileを作るので (B) は使えない | attemptのdirectoryと行の後に消す（段階3） |
| `<DBのstage>.init-lock`（`init` がDBを準備するstageの横） | そのstageを作った `init` のprocessだけ（`CoordinatorStore::initialize_identity`。stageの名前は作ったprocessしか知らず、回復はstageをrenameするだけで、DBとして開かない） | (A)。作ったprocessがDBを閉じた後に自分で消す。killされて残ったものは、名前（stageの名前 + `.init-lock`）が持ち主を証明し、その持ち主が居ないことを確かめて消す（25.4の (a)） | stageと一緒に消す（段階1） |
| `toolchains/<identity>/install.lock` | そのrootへ導入するsetup builder、builderが起動する導入のhelper process、workerの起動時のtrashの片付け | (A)。条件は25.7に書く | rootと一緒に消す（段階4） |
| cloneのlock（`<clone>/.git` の中の `belt-workspace.intent`・`belt-workspace.lock`・`fast-worktree-registration.lock`）と、cloneの横の `<name>.lock`・`<name>.import.lock` | そのcloneのworkspaceを作る・消す処理、bundleの展開、段階5bのrefの削除、単独で動くfast-worktree（0.4.0以降。beltのlockを持たず、inodeを照合しない） | どちらも示せない（25.8の5bの (e)） | 固定。cloneのdirectoryも消さない。個数はcloneの数（`repository_id` の種類）で頭打ち。受け取りを拒否したときの空のcloneの削除は25.9 |
| 親: `startup.lock`、`coordinator.lock`、`init.lock`、`transfer.lock`、`coordinator.db.init-lock`、`upgrade/upgrade.lock`、sourceの変更lock（`sources/mutation.lock`）、`sources/secrets.lock`、Vaultのlock（`secrets/retention.lock`）。worker: `agent.lock`、`worker-owner.lock`、`worker-service.lock`、`worker.db.init-lock`、`group-rotation-key.lock`、`group-rotation-command.lock`、`incoming/locks/<0〜63>.lock` と `incoming/locks/all.lock`（受け取りのlock。source IDのhashを64で割った余りで割り振るので、65個で頭打ち）、sourceの変更lock、`ephemeral.lock`（一時workerのstateだけ。段階1。片付けの直列化は、stateのdirectoryそのものへのflockで行い、lock fileを増やさない） | どの版のどのcommandも開き得る | どちらも示せない | 固定。unlinkしない。個数は増えない |

**(B) と、古い版のCLI。** 基準は「古い版のprocessがまだ開けるlock fileはunlinkしない」である。`reader-pins/<run>.lock` を開く処理は、どれも親DBを開いた後にある。DBは、自分より新しいschemaの版を、読み取り専用でも開かない（`src/storage/policy.rs` の `CompatTable`。新しい版には触れない）。段階0と段階2のmigrationはschemaの版を上げるので、段階2が入った後にpinへ届くのは、段階2以降の版のprocessだけで、その全員が (B) の照合と、段階0の印の確認を行う。PATHに残ったそれより古いCLIは、DBを開けずに終わり、pinのpathに届かない。したがって、段8は基準の文字どおりの読みでも成り立つ。段階2より後の版で、pinを開く経路をDBを開く前に足してはならない。

取得側にinodeの照合を足す段階は、その段階の「schema・wire」に書く。照合を足した版と同じ版では、そのlockを (B) を根拠にunlinkしない（次の版から）。

## 25.2 段階の一覧

| 段階 | 内容 | 前提 | この文面のまま実装できるか |
| - | - | - | - |
| 0 | 既存の挙動の修正。入力の保持の終点の誤り、runの印 `pruning` と断り | なし | できる |
| 1 | crashの受け皿と、持ち主が単純なもの。所有の証明つきの一時file、提出されなかった取り込み（行の無いrunへのcancelを含む）、`in-place/<run>.json`、一時workerの起動し直し | 段階0（印は使わないが、同じmigrationに入れてよい） | できる |
| 2 | runの刈り取り | 段階0、段階1の取り込みの持ち主 | できる（入れてある）。段7（`release_attempt` の送信）は段階3で入れた。親のGit領域のrefを消す部分は、local-ciに関数が無いので入れていない（25.16） |
| 3 | workerの側の結果の写しと、重複防止の記録 | 段階2（`release_attempt` を送る時点が刈り取りにある）。wireの機能 `attempt_release_v1` | できる（入れてある） |
| 4 | 古いbinaryと、toolchainのroot | なし（段階3と独立） | できる |
| 5 | workerが受け取ったsourceとclone | 5a（workerの取り込みの持ち主）は独立。5bは5aの後。wireの機能 `input_missing_v1` | できる（5bの、受け取りを拒否したときの空のcloneの扱いだけは25.9） |

順序の制約: 段階0は他のどれよりも先。段階2は段階1の (b)（取り込みの持ち主）の後に入れる（刈り取りの段5が `capture_owner` の行を扱い、起動時の走査が `sources/runs/` と `in-place/` を「行の無いrunのもの」として消さない規則が、(b) を前提にしている）。段階3のworkerの側の削除は、同じ段階の重複防止の記録と同じ版で入れる（行を消すと、遅れたsubmitを断る記録が無くなる）。

## 25.3 段階0: 既存の挙動の修正

削除の新しい設計は含まない。今のmainにある誤りを直し、後の段階が使う印を足す。

### (a) 入力の保持の終点

**段階0より前の挙動。** 親の入力（source）とVaultの削除は、`input_retention_end_on`（`src/storage/coordinator/retention.rs`）が返す終点を待つ。この章の最初の版の時点で、この式は次の2つのrunで永久に終点を返さず、そのrunの入力は消えなかった。1つ目は、この章と並行して直してある。2つ目は、段階0の (a) で直してある。

- retire済みのattemptを持つrun。retire（`attempt.retired = 1`）はterminalでないattemptにだけ付き（`free_attempt`）、付いた後もterminalにはならない。以前の式は、すべてのattemptに `state = 'terminal'` と閉じた回収を求めたので、満たさなかった。**直してある**: 今の式は `retired = 0` のattemptにだけそれを求め、終点に `retired_at_ms`・taskのcancelの時刻・runを閉じた時刻を入れる（下の規則の3・4と、終点の時刻の文のとおり）。
- attemptを1つも持たないtaskを含むrun（配置前にcancelしたtask、適用済みのdiscoveryのprobe、joinの済んだmatrixの元）。以前の式は「attemptの無いtaskがある」だけで終点なしとした。**直してある**（段階0の (a)）: 今の式は、下の規則の2で判定する。閉じたrunのtaskも、配置されないので済んでいるとする（今のgenerationのattemptを持たないtaskに共通）。

**規則。** runが**終わった**とは、次のすべてが成り立つことである。入力・Vaultの保持の終点と、刈り取りの条件（25.5）は、この同じ述語を使う。

1. どのtaskにも、terminalでなくretireも確認されていないattempt（`retired = 0 AND state <> 'terminal'`）が無い。
2. どのtaskも済んでいる。済んでいるとは、今のgenerationのattempt（`retired = 0 AND generation >= task.min_generation` のうちgenerationが最大のもの）がterminationを持つこと、またはそのようなattemptが無く、taskがcancel済み・適用済みのprobe・joinの済んだmatrixの元のどれかであるか、runが閉じている（`run.closed_at_ms` がある）こと。閉じたrunはtaskを配置しないので、今のgenerationのattemptを持たないtaskは、閉じた時点で済んでいる。`workflow_run_end`（同じfile。workflow artifactの保持の終点）も、taskが済んでいるかをこの同じ述語で判定する（閉じたrunの場合を含む）。開いているrunで配置を待つtask（queueに残るもの）は済んでいない。
3. `retired = 0` のattemptはすべて、`observation = 'known'` で、型のあるterminationを持ち、回収が閉じている（`collection.closed_at_ms` があり、verdictが `complete`・`required_missing`・`gave_up` のどれか）。
4. `retired = 1` のattemptには、3を求めない。`retired = 1` を書くのは `free_attempt` だけで、それを呼ぶのは、retireの確認（`free_by_retire`）と、workerが「何も保存していない」と答えた配置を閉じるとき（`close_unstored`）の2つである。どちらも、同じtransactionで `retire_tombstone` に確認の時刻を持つ。したがって `retired = 1` は、それ自体が「このattemptはworkerで実行されない、またはその結果を親は使わない」の記録である。

終点の時刻は、`retired = 0` のattemptのterminalと回収を閉じた時刻、`retired = 1` のattemptの `retired_at_ms`、taskのcancelの時刻、runを閉じた時刻のうち、最も遅いものとする。

**安全の証明。** 入力を読むのは、配送、workspaceの準備、`retry` である。retire済みのattemptは配送もされず、準備もしない。`retry` は新しいgenerationを作り、作った時点で1と2が崩れるので終点が消える（既存のsourceのguardが、retryと削除を直列にする。第23章）。したがって、4で緩めても、読み手の居る入力は消えない。

**受け入れ試験。** (i) retire済みのattemptと、成功した次のattemptを持つrunで、終点が返る。(ii) 配置前にcancelしたtaskだけのrunで、終点が返る。(iii) terminalでなくretireも確認されていないattemptが1つでもあれば返らない。(iv) queueに残るtaskがあれば返らない。(v) `retired = 0` のattemptがunknownなら返らない。(vi) retire済みのattemptの `retired_at_ms` が終点に入る。(vii) 既存のretryと削除の排他の試験が通る。

### (b) runの印 `pruning`

**規則。** 親DBの `run` に、刈り取り中の印（`pruning_at_ms`。NULLでなければ印あり）を足す。印は取り消さない。印を書くのは段階2の刈り取りだけで、段階0では誰も書かない。

- **書き込みの断り。** 印のあるrunへの `retry`・enqueue・`create_task`・`create_attempt`・cancelは、`NOT_FOUND`（理由 `run_pruned`）で断る。印の確認は、それぞれが行やattemptを作るのと同じtransactionで行う。runに属する行を書く他の書き手も、同じtransactionの中で同じように断る: CIのdriverの記録とmatrixの元のjoin、event（attemptだけを名指すeventは、attempt→task→runとたどって確かめる）、配置の記録、matrixのgroupとpermit、回収・logの進み・artifact・workflow artifactの行。印の後にこれらの行が増えると、刈り取りが通り過ぎた表に行が残る。
- **印の確認は、再送の受領より先。** cancelと `retry` は、同じ `request_id` の再送に、保存した答え（`control_request` の行）を返す。段階0より前の実装は、対象を解決する前にこの受領を引いた（`src/storage/coordinator/control.rs` の `apply_cancel` の最初の文、`src/storage/coordinator/retry.rs` の3か所）。段階0で順序を入れ替えてあり、対象のrunの印を先に確かめる。印があれば、受領の行が残っていても `NOT_FOUND`（`run_pruned`）と答える。受領の行は、刈り取りがrunと一緒に消す（25.5）ので、「刈り取りの途中は保存した答え、刈り取りの後は `NOT_FOUND`」と答えが変わる窓を作らない。
- **読み手の断り。** 印のあるrunは、すべての読み手に `NOT_FOUND` と答える: `show`、`ls`（一覧に出さない）、`logs`、`outputs`、`top`、`ci status`（`--post` を含む）、GitHub statusの再投稿、residentの復旧と配置の走査。読み手は、印の確認と、答えに使うrunの行の読み取りを、**1つの読み取りtransaction**（同じsnapshot）で行う。複数の問い合わせで答えを組み立てる読み手（`show`、`ci status --post`）は、その全部を同じtransactionの中で行う。**段階0の実装は、これを次の形で満たす**（wireを変えないため）: `show` は既存の2つの問い合わせ（`Run`、`Tasks`）のままで、それぞれが自分のsnapshotで印を確かめる。`ci status` と、taskについて複数回読む問い合わせ（attemptの一覧と回収の状態、logとoutputのmetadata）は、最初の読み取りの前に印を確かめ、最後の読み取りの後に、1つのsnapshotで「runの行（taskを名指す問い合わせではtaskの行も）があり、印が無い」ことを確かめる。印は取り消されず、行は印の後にだけ消えるので、後の確認が通れば、それより前の読み取りはすべて欠けの無いrunを見ている。通らなければ（印がある、または刈り取りが最後まで進んで行が無い） `NOT_FOUND` と答え、途中の読み取りの結果は使わない。
- **fileの読み手は、pinを取った後に確かめ直す。** `logs`・`outputs` は、共有pinを取った**後**に、1つの読み取りtransactionで、印が無いことと、対象がそのrunのものであることを確かめ直す。確かめ直しが通らなければ、pinを手放して `NOT_FOUND` と答え、fileを開かない。段階0より前の実装（`src/storage/coordinator.rs` の、pinを取る関数）は、pinの後に対象のrunだけを確かめ直していた。段階0で、同じsnapshotに印を足してある。pinを取る前の確認だけでは足りない: 確認とpinの間に印が書かれ、刈り取りが排他pinを取ってfileを消し、pinを手放した後に、読み手が共有pinを取れてしまう。

**決定: 「印のあるrunは無い」とする。** 採らなかった案は「印から最後まで排他pinを持ち続ける」である。pinを持ち続ける案は、(1) passをまたいで、さらにresidentの再起動をまたいでdescriptorを持ち続けることはできず、再起動の直後に必ず窓が開く、(2) pinを取らない読み手（`show`・`ls`・`ci status`）には効かない、(3) fileの読み手が「無い」ではなく待ちか失敗になる。印はDBに永続し、読み手のsnapshotの中で確かめられるので、半分消えたrunを見る経路が無い。

**安全の証明。** 刈り取りの各transaction（25.5）は印のあるrunの行だけを消す。読み手のsnapshotは、印の前のものならrunの全部の行を含み、印の後のものなら印を見て `NOT_FOUND` と答える。行の一部が欠けたrunを、印なしで見るsnapshotは存在しない。fileの読み手（`logs`・`outputs`）は、これに加えて共有pinを持ち、刈り取りは排他pinを取れたときだけfileを消す（25.5）。読み手の順序は「共有pin → 印の確認 → fileを開く」、刈り取りの順序は「印 → 排他pin → fileを消す」である。読み手の確認が印より前のsnapshotなら、読み手は共有pinを既に持っているので、刈り取りは排他pinを取れず、読み手が開いたfileは、pinを持つ間は消えない。ただし、pinを持つ読み手も、印の後の次の行の読み取り（完了の待ち、logとoutputのmetadata）には `NOT_FOUND` が返る（行は、排他pinを必要としない段で先に消え得るので、断るのが安全な側である）。したがって、追いかけて読む `logs -f` や、複数回に分けて読む `outputs` は、印が書かれた後、途中で `NOT_FOUND` で止まり得る。印より後のsnapshotなら、読み手は印を見てfileを開かない。どちらでもない順序は無い。再送の受領を印の後に引くので、印のあるrunについて、保存した答えが返る経路も無い。

**受け入れ試験。** 印を直接書いたrunで: (i) `retry`・enqueue・`create_task`・`create_attempt`・cancelが `run_pruned` で断られ、行が増えない。(ii) `show`・`logs`・`outputs`・`ci status --post` が `NOT_FOUND` で、`ls` に出ず、GitHubへ何も投稿しない。(iii) taskの行を半分消した状態でも (ii) が同じ答えになる（`missing` と判定しない）。(iv) 印の無いrunは今までどおり。(v) cancelと `retry` を受理して受領の行を作った後に印を書き、同じ `request_id` で送り直すと、保存した答えではなく `run_pruned` が返る。(vi) `logs`・`outputs` が、最初の確認を通ってから共有pinを取るまでの間に、印を書いて排他pinでfileを消して手放す。読み手は `NOT_FOUND` と答え、消えたfileを開かない（確認とpinの間に止める点を差し込んで確かめる）。

**schema。** 親DB: `run.pruning_at_ms INTEGER`。wireの変更は無い。

## 25.4 段階1: crashの受け皿と、持ち主が単純なもの

### (a) 一時fileと一時directory

**今の挙動（段階1の (a) の実装後）。** 規則と、表の各場所の走査は入った（名前は `owned_temp::file_in`・`new_file_name`、走査は `sweep_files`、directoryは `OwnedTemp` と `sweep`、作りかけのdirectoryだけを見る `sweep_unfinished`。journalが名指すstageは `stage_in`・`new_stage_name` と、`init` だけが呼ぶ `sweep_stages`。`doctor` は残った件数を `leftover_temporaries` で示す）。規則の外に残るのは2つで、どちらも所有を証明する名前にしていない: 再開できる受け渡しの `incoming-*.part`（`worker/ci_task.rs`。名前が決まっていて、持ち主の `sweep_plans` が消す）と、`setup/store.rs` の `.tmp-<ulid>`（workerの起動時の `sweep` が、`agent.lock` の下で消す）。他のuidのfileに触れないことは、`lstat` の結果（種類・link数・uid）だけから決める関数（`Seen::is_own_file`・`is_own_directory`）に、別のuidを与えて確かめる（rootでなければ別のuidのfileを作れないので、fileそのものでは確かめない）。

**規則。** beltが作る一時のfile・directoryは、例外なく次のどちらかにする。

- **directory**は、所有marker（`platform::owned_temp`。場所・pid・processの開始identity）を持つ。
- **markerの無いdirectoryを、その名前で見せない。** directoryは、単独のfileと同じ形の名前（`.belt-tmp.…`。下）で作り、中にmarkerを書いてfsyncしてから、置き換えをしないrenameで本来の名前（`<接頭辞><乱数6字>`）にする。途中でkillされたprocessが残すのは `.belt-tmp.…` の名前のdirectoryで、名前が持ち主を証明するので、directoryの走査（`sweep`・`sweep_unfinished`）と単独のfileの走査（`sweep_files`）の両方が、持ち主の居ないものを中身ごと消す。したがって、本来の名前のdirectoryのmarkerが空・途中までであることは、killでも電源断でも起きない。そういうmarkerは誰のものでもないものとして残す。持ち主を言えないprocess（下）は、今までどおり本来の名前で直接作り、markerを書かない。
- **場所は、hostの名前ではなく、機械のidentityで決める。** 場所は、機械のidentity・PID namespace・OSのbootの3つである。機械のidentityは、macOSでは `gethostuuid` のhost UUID、Linuxでは `/etc/machine-id` で（`/var/lib/dbus/machine-id` は読まない: imageに焼き込まれていることが多く、同じimageのcontainerがどれも同じ値を持つ）、32桁のhex（全部0は不可）として読めたものだけを使う。hostnameは使わない: 同じ名前の2つのhostが1つのdirectoryを共有すると（NFSのhome、複製したVM、`hostname:` を固定してhostのPID namespaceを使うcontainer）、LinuxのPID namespaceの番号は最初のnamespaceならどのhostでも同じなので、片方の起動時の走査が、もう片方の生きている一時fileを「同じ場所の別のboot」と読んで消してしまう。hostnameを変えても場所は変わらないので、名前を変える前の残りも、同じ機械の走査が消す。markerには `machine:<32桁>` の行で書く。hostnameを書いた以前の版のmarker（5行で、4行目がhostname）は、機械を言っていないので、記録として扱わない（残す）。
- **機械のidentityを共有する環境は対象外とする。** 同じ `/etc/machine-id` を持つVMの複製や、hostの `/etc/machine-id` を見せたcontainerどうしが、1つのdirectoryを共有し、PID namespaceの番号も同じ場合、互いを区別する手段が無い。この構成では、beltのstateとconfigを共有しないこと。
- **単独のfile**（renameで公開する前のfile、切替の途中のlink）はmarkerを持てないので、**名前**が所有を証明する。名前は `.belt-tmp.<場所のhash>.<bootのhash>.<pid>.<開始identity>.<乱数>` とし、markerと同じ事実（場所・boot・pid・開始identity）を運ぶ。それぞれの定義は `owned_temp` と同じものを使う。
- **場所とbootは、別々の固定長のhashで運ぶ。** `owned_temp` の場所は、機械のidentity・PID namespace・OSのbootの3つで、そのまま並べると長く、bootのIDの形はOSごとに違う。名前に入れるのは、機械のidentityとPID namespaceを区切りつきで並べたもののSHA-256の先頭16桁（hex。場所のhash）と、bootのIDのSHA-256の先頭8桁（hex。bootのhash）である。bootを場所のhashに混ぜない: 混ぜると、再起動の後は「別の場所」と区別できず、電源断で残った単独のfile（鍵の平文の写しを含む）を、どのprocessも二度と消せない。pidは10進（10桁まで）、開始identityは10進（20桁まで。0は使わない）、乱数は英数字12字とする。名前は最長で `.belt-tmp.`（10）+ 16 + 1 + 8 + 1 + 10 + 1 + 20 + 1 + 12 = 80 bytesで、どのfilesystemの上限にも収まる。走査は、今の場所から同じ2つのhashを計算して比べる。場所のhashが違えば「場所が違う」として触れない。場所のhashが同じでbootのhashが違えば、持ち主はそのbootと一緒に居なくなっている（markerの規則と同じ）。bootのhashも同じときだけ、pidと開始identityを見る。directoryのmarkerは、fileの中身なので、今までどおり3つをそのまま持つ。
- **持ち主を言えないprocessの名前は、誰のものでもない。** 場所（機械のidentity・PID namespace・boot）か、自分の開始identityを読めないprocessは、2つのhashを全部0、開始identityを0にした名前を使い、directoryにはmarkerを書かない。開始identityが0の名前は解釈できない名前として扱い、走査は触れない（作ったprocessだけが消す）。本物の場所に、作り物の開始identityを組み合わせない: 読み手には「持ち主が居ない」と見えてしまう。
- **journalに書いた名前のfileは、一時fileではない。** `init` は、公開の直前のfile（鍵・binary・DB・入力・serviceの定義）の名前を `init.json` の `pending` に書いてから公開し、途中で終わると、後の `init` がそのfileを使って公開を済ませる。後のprocessへ引き渡すfileなので、「読むのは作ったprocessだけ」という一時fileの前提が成り立たない。このfile（stage）は `.init-` で始まる名前にし、一時fileの走査は見ない。持ち主はjournalの回復である: 回復が公開を済ませる（renameで名前が消える）か、取り消しがjournalと一緒に消す。名前の残りは一時fileと同じ形（`.init-<場所のhash>.<bootのhash>.<pid>.<開始identity>.<乱数>`）にして、作ったprocessが分かるようにする。`init` は、`init.lock` の下で回復が済み、`pending` が空になった後に、そのjournalだけが扱うdirectory（config directory、binaryのdirectory）にある、持ち主の居ないstageを消す（journalに書く前に終わった `init` の残り。journalが名指していないことは、`pending` が空であることで分かる）。親のstateは、config directoryとは別に決まる（`XDG_STATE_HOME`）ので、別のconfig directoryの `init`（別の `init.lock` とjournal）が同じstateを見ることがあり、そのjournalがstateのDBのstageを名指しているかもしれない。そこで、親のstateのstageは、**自分のjournalがそのstateの `coordinator.db` を公開済みで、そのfileが今もそこにあるとき**（journalの証明とinodeが一致するとき）だけ消す。その間、別のjournalが名指すstageは決して公開できない（公開は置き換えをせず、回復が既にあるtargetを受け入れるのは、それがstageと同じfileのときだけである）ので、消しても誰の回復も妨げない。DBを公開した `init` は、公開の直後にもう一度この走査を行う（最初のDBの準備中にkillされた `init` の残りを、同じ実行の中で消す。3回目の `init` を待たない）。DBのstageの横には、SQLiteのjournal（`-wal`・`-shm`・`-journal`）と準備のlock（`.init-lock`）ができる。どれもstageの名前にこの4つの語尾を付けた名前なので、stageと同じ持ち主の証明で、同じ走査が消す。stageをDBとして開くのは作ったprocessだけ（回復はrenameするだけ）なので、作ったprocessは、DBを閉じた後、journalに名前を書く前に、この4つを自分で消す（lock fileの規則の (A)。25.1）。公開の後に横に残るものは無い。serviceの定義のdirectoryは、同じ利用者の別の導入（別のconfig directoryとjournal）と共有で、stateのような持ち主の印が無いので、そこのstageは消さない。古い版が書いた `.init-<ulid>` は、誰が作ったか分からないので消さないが、`pending` が名指すstageとしては、どちらの形も受け付ける（古いbinaryの書いたjournalを回復できる）。
- 作った時点でunlinkできるもの（匿名で足りるもの）は、名前を持たせない。

走査は、その場所で同じ種類の処理を始めるとき（下の表）に、そのdirectoryの `readdir` 1回で行う。消すのは、名前かmarkerが解釈でき、場所が今と同じで、bootが今と違うか、そのpidのprocessが居ないか開始identityが違うものだけである。自分のuidの所有で、link数が1（fileのとき）で、symlinkをたどらない。解釈できないもの、場所が違うものには触れず、`doctor` が件数を示す。processにsignalは送らない。

| 場所 | 作るもの | 走査の契機 |
| - | - | - |
| 親のstate直下（`<state home>/belt/coordinator/`。workerのstateとは別のdirectory） | `ci-plan-*/`（計画の下書き）、`traffic-hold.json` などの固定fileの一時file。DBの準備の `.init-*` はstageで、一時fileではない（上の規則） | 計画の開始、`init`・`upgrade`・`group`・`workers` の各commandの開始、residentの起動 |
| 親・workerの `sources/tmp/` | `.recv.*`、`.manifest.*`。以前の版が残した `commit-<sha>-*/`（この版は、commitの取り込みのcheckoutを、取り込みのscratch directory `sources/staging/<ulid>/` の中に作る。(b)） | そのstoreの変更lockを排他で取れたpass（親は保持driver、workerは起動時） |
| 親の `results/<attempt>/tmp/` | `*.part` | 回収の続き（既存）。attemptの結果を消すときに一緒に消える |
| 親の `secrets/` | `.<名前>.<ulid>.tmp` | Vaultの排他lockの下の保持driver（既存の `sweep_orphan_tmp`。所有を証明できる名前に変える） |
| 親の `sources/secrets/` | `.secrets.tmp-*`（local-ciが書く、平文のsecretの束の一時file。名前はlocal-ciが決めるので、この規則の名前にできない） | 保持driverが `sources/secrets.lock` を待たずに排他で取れたpass。この一時fileを書く取り込みは、必ずこのlockを持つ（`src/secrets/capture.rs`）ので、lockを取れた時点の残りは、書き手の居ない残骸である。workerの側は、起動時の既存の片付け（`sweep_store_copies`）が同じ規則で消す |
| workerのstate直下 | `.s*/`（socketの準備）、`worker-socket.json` などの一時file、`removed-members.json` の一時file | workerの起動（`agent.lock` の下） |
| workerの `clones/`・`workspaces/` | 展開と作成の作業用directory（`.clone.*`・`.staging.*`） | workerの起動（既存の孤児の走査）。`agent.lock` の下で、どの作成よりも前に走るので、書き手は居ない。markerのあるものは持ち主の規則に従い、markerの無いもの（古い版が作ったもの、作ったprocessが自分の場所か開始identityを読めなかったもの）は、書き手が居ないことを根拠に消す。markerがあって読めないものは残す |
| workerの `setup/builders/<id>/`、`attempts/<attempt>/`、親の `results/<attempt>/` | builderの一時file、準備の観測の一時file、CIのartifactの受け渡しの作業用 | 持ち主のdirectoryと一緒に消える（既存）。持ち主のdirectoryの外（state直下）に書くものは、workerの起動 |
| workerの `attempts/<attempt>/ci/exchange/` | `.belt-xchg.*/`（artifactのstep 1つの作業用directory） | 同じattemptの次のartifactのstepの開始（`scratch_in`）。次のstepが無ければ、attemptのdirectoryと一緒に消える |
| system userのworkerの入力のdirectory（管理者が決めたconfig directory） | 入力・journalの一時file、`bin/` の切替の途中のlink | そのworkerの入力を書くcommand（`worker-service`）の開始と、そのworkerの起動。`install` と起動は、自分が解決したdirectory（起動は、渡された入力のあるdirectory）を走査する。`worker-service` のほかのactionは、同じ環境変数（`HOME`・`XDG_CONFIG_HOME`）から同じdirectoryを解決する、commandの開始の走査による |
| config directory | `init.json`・`membership-undo.json`・`worker-service.json`・鍵・設定の一時file | `init`・`group`・`workers` の各commandの開始、residentとworkerの起動 |
| `<config>/upgrade/` | `record.json` と退避した設定の一時file | `upgrade` の開始、serviceの入口 |
| `<config>/bin/` | `.resident.<pid>`、`.current.<pid>`（切替の途中のlink） | `upgrade`・`init` の開始、residentとworkerの起動 |
| LaunchAgentsのdirectory（macOS）、systemdのunitのdirectory | service定義の一時file | serviceの定義を書くcommandの開始 |
| `$TMPDIR` | `belt-log-reader-*/`、profileの検査用directory | `logs` の開始、検査の開始 |

LaunchAgentsとunitのdirectoryは他の製品と共有なので、`.belt-tmp.` で始まる名前だけを見る。一時fileの名前は `.plist`・`.service` で終わらせない（service managerが読み込まない）。

**安全の証明。** 一時fileを読むのは作ったprocessだけである（公開はrename）。そのprocessが居ないことを、場所・boot・pid・開始identityで確かめる。場所が同じでbootが違えば、そのbootのprocessは1つも残っていない。bootも同じなら、pidが使い回されていても、開始identityが違えば別のprocessである。場所が違うfile（別の機械、別のPID namespace、または機械を特定できない）は消さない。

**前提と、証明できずに残るもの。** どれも、消せる根拠が無いので残す。`doctor` の `leftover_temporaries` は、config・state・serviceの定義のdirectoryにある `.belt-tmp.*` の件数を示す。

- **processの読み取り。** 走査は「そのpidのprocessが居ない」というOSの答えを信じる。Linuxで `/proc` が `hidepid`（`hidepid=invisible`、systemdの `ProtectProc=invisible`）で他のprocessを隠していると、生きているagent（core dumpを止めているので、同じuidからも見えない）が「processなし」に見える。第15章の既知の制約と同じく、この構成は対象外とする。
- **機械のidentityを読めない環境。** `/etc/machine-id` が無い・空のcontainer imageでは、どのprocessも持ち主を言えないので、名前は誰のものでもない形になり、directoryにmarkerは付かない。crashで残った単独の一時fileは、永続volumeの上に残り続ける（crash 1回につき、そのprocessがその時点で書いていた一時file。鍵・設定・journalの写しで、1つは数KBまで。sourceの受け取りの途中のfileはそのobjectの大きさ）。markerの無いdirectoryのうち、書き手が居ないことをlockで示せる場所のもの（workerの `clones/`・`workspaces/`）は、今までどおり起動時に消える。それ以外の場所のmarkerの無いdirectory（`ci-plan-*/`、`.s*/`、`.belt-xchg.*/`、`belt-log-reader-*/`）は、crashで残ると残り続ける。どれも数KBから数MBである。commitの取り込みのcheckout（取り込むcommit 1つ分のtree）は、取り込みのscratch directoryの中にあり、markerではなく `capture_owner` の行が名指すので、放棄の処理が消す（(b)。持ち主を言えないprocessの取り込みは、`given_up` でだけ放棄になる）。`.belt-xchg.*/` はattemptのdirectoryと一緒に消える。消すには、imageに機械ごとのmachine-idを持たせる。
- **作り直したcontainer。** containerを作り直す（または再起動する）とPID namespaceが新しくなり、その番号が変われば場所が変わる。前のcontainerが永続volumeに残した一時file・directoryは「別の場所」のものになり、消えない（量は上と同じで、crashの回数に比例する）。新しいcontainerからは、前のnamespaceが無くなったのか、同じvolumeを共有する別のcontainerとして今も動いているのかを区別できないので、証明が無い。hostのPID namespaceを使うcontainerでは起きない。
- **以前の版のmarker。** hostnameを書いたmarkerと、markerを書く途中でkillされた以前の版のdirectory（markerが空・途中まで）は、記録として読めないので残る。workerの `clones/`・`workspaces/` でも「markerがあって読めないもの」として残る。増えるのは以前の版が動いていた間のcrashの分だけで、この版からは増えない。1つの大きさは場所による: `clones/.clone.*` はclone 1つ分、`workspaces/.staging.*` はworkspace 1つ分、`sources/tmp/commit-*/` はcommit 1つ分のtreeで、ほかは数KBから数MBである。以前の版をcrashさせたことのあるhostでは、更新の後にこれらを手で消す。

**受け入れ試験。** 表の各場所について: (i) 一時fileを作ったprocessをSIGKILLし、次の契機でそのfileが消える。(ii) 生きているprocessの一時fileは消えない。(iii) pidが同じで開始identityが違うものは消える。(iv) 名前が合わないfile、他のuidのfile、link数が2のfileには触れない。(v) LaunchAgentsのdirectoryにある他の製品のfileに触れない。(vi) `src/` に、この規則を通らない `tempfile` の呼び出しが無いこと（testを除く）を、固定の検査で確かめる。検査は、`#[cfg(test)]` の付いた項目だけを、その項目の終わり（`;` か、対応する閉じ括弧）まで除き、`tempfile` をpathの一部として使う行と、`use tempfile as …` のような別名を見つける。文字列と `/* */` の中は解釈しない。 (vii) journalが名指すstageは、作った `init` が居なくなった後の各commandの開始の走査で消えず、次の `init` が公開を済ませる。 (viii) 場所が同じでbootが違う名前のfileは消え、場所が違う名前のfileは消えない。 (ix) 機械のidentityだけが違う場所（PID namespaceは同じ、bootは別、pidは空いている）の名前のfileとmarkerのdirectoryは消えない。hostnameを書いたmarkerは記録として読まれない。 (x) 別のconfig directoryの `init` が、共有するstateにある、他のjournalが名指すDBのstageを消さず、元の `init` が回復できる。最初のDBの準備中にkillされた `init` のstageは、次の `init` がDBを公開した実行の中で消える。 (xi) DBのstageの準備が済んだ時点で、stageの横に `-wal`・`-shm`・`-journal`・`.init-lock` が無い。killされた `init` のstageの横のこの4つは、stageの走査で消え、一時fileの走査では消えない。 (xii) `.belt-xchg.*/` は、持ち主の居ないものが次のstepの開始で消え、生きている持ち主のものは残る。 (xiii) markerのdirectoryは、本来の名前を得る時点でmarkerが完全で、その前には本来の名前のものが無い。作りかけ（`.belt-tmp.…` の名前のdirectory。中身は空・`d` だけ・途中までのmarker）は、持ち主が居なければ3つの走査のどれでも消え、生きている持ち主のもの、別の場所のもの、本来の名前で途中までのmarkerを持つものは残る。 (xiv) 他のuidのfile・directoryは、名前やmarkerが何を言っていても、調べる対象にならない（`lstat` の結果にuidを与えて確かめる）。

### (b) 提出されなかった取り込み

(b) と (c) は実装してある。下の「段階1より前の挙動」は、この規則が入る前の挙動である。

**段階1より前の挙動。** 取り込みはCLIのprocessの中で、runの行ができる前に行われる（`src/service/submit.rs`・`src/service/ci_run.rs` → `service::input::capture` → `secrets::capture::capture_run` → `source::capture_for_run`）。順序は次のとおりである。

1. `sources/secrets.lock` を取る（secret fileがあるとき）。
2. `sources/runs/<run>.lock` を作って取る（`lock_run`）。
3. pending root（`capture:<ulid>`）を作る。local-ciの取り込みが、objectとmanifestを書き、平文のsecretの束を `sources/secrets/<run>.json` に書く（途中は `.secrets.tmp-*`）。
4. runのroot（`root` の行。`id = <run>`、`kind = 'run'`）を作り、pending rootのmemberを写す（`copy_root`）。
5. `sources/runs/<run>` を書く（`bind_run`）。
6. pending rootのmemberを外し、pending rootを消す（`handoff`。pageに分かれる）。
7. 平文の束を親Vaultの `secrets/<run>.json` へ移す（`Vault::adopt`）。
8. `in-place` のrunでは `in-place/<run>.json` を書く。
9. enqueueを送る。

enqueueの前にCLIがCtrl-Cやcrashで終わると、どれも消えない（消すのは、提出が断られたときの `in-place/<run>.json` と、取り込みが失敗したときの平文の束だけ）。2と3の間で終わればlockだけが残り、6の後で終わればpending rootは無く、runのrootとそのmember・graphを名指して消すものが無い。親では、残った平文の束を片付ける処理が呼ばれていない（`sweep_store_copies` を呼ぶのはworkerだけである）。secretの束が際限なく残る。

また、runの行が無いrunへのcancelを、段階1より前の実装は受理した（`control.rs` の `resolve` は、`Target::Run` を行の有無を見ずに通す）。取り込みの途中のCtrl-Cは、まだenqueueしていないrun IDへcancelを送り（`ci_run.rs`、`service/control.rs` の `cancel_and_stop`）、`run_cancel_intent`・`run_id` つきの `event`・`control_request` の行ができる。これは意図した挙動で（後から届くenqueueがcancelを引き継ぐ。`storage/tests.rs` の `run_cancel_intent_precedes_enqueue_and_every_creation_and_place_gate`）、enqueueが届かなければ、この3つの行を消すものが無い。利用者が打ち間違えたrun IDへのcancelでも同じ行ができる。

**規則。** 取り込みに持ち主を与える。

- **持ち主の行が、最初の永続の書き込みである。** 取り込みを行う提出（`submit` と `ci run`）は、run IDを決めた直後、上の1より前に、`sources/retention.db` の `capture_owner` に1行を書く: run ID、状態（`capturing`）、pending rootのID（まだNULL）、場所、pid、processの開始identity。この行がcommitされるまで、lockもfileも作らない。pending rootのIDは、pending rootを作るのと同じtransactionで、この行に書く。storeそのものの固定のfile（`retention.db`、`mutation.lock`、directory）は、行より先にある（行はその `retention.db` に書く）。`capture_owner` の表は、`retention.db` を次に開いたときに足し、その版（`user_version`）は上げない: 更新のrollback（24.8）は `retention.db` を戻さないので、版を上げると、戻した古い版がこのfileを開けなくなる。古い版は、知らない表を読まない。同じprocessが同じrunをもう一度取り込むとき（取り込みはrunにつき1回で、2回目は同じsourceを返す）は、自分の行をそのまま使う。別のprocessの行、`given_up` の行があるrun IDの取り込みは断る。行が名指すpending rootがまだ在る間（同じrunの先の取り込みが、下の6より前に失敗した後）は、2つ目のpending rootを作らずに取り込みを断る（rootも行も書かない。確認と書き込みは1つのtransaction）。行が名指せるrootは1つで、上書きすると、先のrootを名指すものが無くなる。名指すものの無いpending rootは `source` を持たないまま残り、Bのpage・Vaultのpage・previewと、放棄の処理のobjectの削除を止め続ける。断られた取り込みの持ち主は `given_up` を書いて終わり、放棄の処理が、行の名指す先のrootを消す。行が名指すrootがもう無い（引き渡しが消した）ときと、持ち主の行が無い取り込み（計画の下書き）は、断らない。持ち主の行を書いた取り込みのpending rootは、その行が `capturing` のまま名前を受け取れたとき（同じtransactionの `UPDATE` が、ちょうど1行を変えたとき）にだけ作る。行が `given_up` か、もう無いときは、transactionを取り消して取り込みを断る（rootを書かない）: Ctrl-Cは進行中のblockingの取り込みを止めないので、`given_up` を書いた後も、processが終わるまで取り込みは進む。その間に放棄の処理が行を消すと、後から作るpending rootを名指すものが無くなる。行を書いたかどうかは、行を書いたstoreのhandleが覚えている（行が消えた後は、表からは判らない）。別のhandleからでも、`capturing` でない行があるrunのpending rootは作らない。先のrootを同じtransactionで `abandoned` にする案は採らない: そのrootのmemberとobjectを消す起点（行のID）が無くなり、rootの行が残る。
- **取り込みのscratch directoryと予約。** 取り込みは、storeの新しいfile（objectとmanifest）を、storeへ直接書かない。local-ci 0.19.0の、段階を分けた取り込み（`capture_source_staged`・`capture_directory_staged`）を使い、次の順で書く。
  1. pending root（`capture:<ulid>`）をcommitする（上のとおり、持ち主の行があれば同じtransactionでそのIDを行に書く）。
  2. scratch directory `sources/staging/<ulid>/` を作る（0700。`<ulid>` はpending rootのIDのもの）。名前はpending rootのIDから決まるので、directoryが在る時点で、それを名指す行は既にcommitされている。既に在る名前は使わない（取り込みを断る）。commitの取り込みのcheckout（`commit-<sha>-*/`）も、この中に作る（以前は `sources/tmp/`）。
  3. local-ciは、読んだ内容・manifest・一時fileを、すべてscratch directoryの中に書く（`objects/`・`manifests/`・`tmp/`。`secrets/` も作るが、束のpathを指定する取り込みでは空である）。入力が動いてやり直した回の分は、local-ciが次の回の前に消す。この間、storeには何も書かれない。local-ciは、取り込みの間、scratch directoryの `flock` を持つ。
  4. 内容が安定してmanifestが決まった後、local-ciは、storeへ入れるfileの一覧を1回だけ渡す（`before_install`）。beltは、その全部を、1つのtransactionで、pending rootのmemberとして書く（**予約**。`publication`・`object`・`member` の行で、受け取りの `import_blob` と同じ形）。削除のclaimが付いているobjectがあれば断り、何も入れない。宛先が、索引の決めるpath（`objects/<aa>/<hash>`、`manifests/<source>.json`）でないものも断る。
  5. local-ciが、各fileを、置き換えをしないhard linkでstoreへ入れ、scratchの名前を消す（objectが先、manifestが最後）。既に在る宛先は、内容を全部読んで確かめ、そのまま使う。内容が違えば、入れるのをそこで止めて失敗する。
  6. beltが、scratch directoryを中身ごと消す。消せなければ取り込みは失敗する（引き渡しの後は、directoryを名指す行が無くなるため）。失敗で戻るときも消すが、そのときの消し残しは、放棄の処理が消す。取り込みは、自分のscratch directoryの `flock` を確かめずに消す（local-ciが戻った後なので、lockは放されている。確かめると、別のthreadがforkした子がexecするまで持つdescriptorの写しを、使用中と読み違える）。
  7. 今までどおり、manifestのgraphをpending rootに登録する（`register_held`）。
  - **予約は、消してよい根拠を増やさない。** fileを消してよいかは、今までどおり「他のrootがmemberに持たないこと」と、claimの検証で決まる。取り込みが、既に在ったfileをそのまま使った（内容が同じだった）ときも、予約はmemberの1行で、他のrootのmemberを減らさない。
  - **入れる途中で止まったとき**（宛先に別の内容のfileがある、diskが一杯）、local-ciは、この取り込みが作ったfileの一覧を付けて失敗を返す。beltは、その一覧に無い予約を、その場で外す: 既に在って内容が同じだったfile（一覧の「既に在った」）は、この取り込みのものではなく、届かなかったfileは存在しない。作ったfileの予約は残し、放棄の処理が消す。止まった宛先（入れる順で、作ったものにも既に在ったものにも入らない最初のfile）の予約も残す: そこに内容の違うfile（以前の版がfsyncせずにrenameし、電源断で欠けたものなど）が在ると、どのrootも名指さないまま、同じ内容の後の取り込みが毎回そこで止まるからである。宛先の名前は手掛かりにすぎず、消すかどうかは放棄の処理が、sourceの排他の下で自分で決める: 宛先が無ければ予約を外し、内容が合えば通常どおり扱い、自分の通常のfileでlink数が1で、全部を安定して読めて内容が名前と違う（objectはdigestが違う、manifestは読めない・sourceが違う・digestの検証を通らない）ときは、他のrootが持たなければ、そのfileの実際の内容でclaimを書いて消す。読めない、link数が2以上、他のuid、途中で変わったものは、今までどおり残す。他のrootが持つfileは、内容が違っても消さず、予約だけを外して数える（放棄の処理の報告の `corrupt_held`。件数は索引の `meta` の1行にも足され、`doctor` が「beltは持たれている間は消せない」と添えて示す）。予約を外す書き込みは、そのobjectを他のrootもclaimも持たなければ、同じ取引の中でobjectとpublicationの行も消す。予約を外す書き込みに失敗したときは、予約が残り、放棄の処理が同じ規則で扱う。
  - **sourceで名付けた束は予約しない。** secret fileがあるのに束のpathを決めていない取り込み（local-ciが、storeの `secrets/<source>.json` に束を書く形）では、束も「入れるfile」の一覧に入るが、beltはこれを予約しない。束は索引のobjectではなく（種類が無い）、今までどおり、呼び出し元が引き取るまで残り、crashで残った分は `sweep_store_copies`（束を書く取り込みと同じlockの下）が消す。製品の取り込みは、必ず束のpath（`sources/secrets/<run>.json`）を指定するので、この形の束を作らない。
  - **対象外。** 親のGit領域 `sources/git/<key>.git` のobjectとrefは、local-ciが今までどおり直接書く。途中で終わった取り込みが書いた分も、他のrefと同じく残る（25.16）。
- **計画の下書きは対象外。** 計画のための取り込み（`service/planning.rs` の `capture_into`）は、下書きのdirectory（`ci-plan-*/`）の中の、そのcommandだけのstoreへ書く。持ち主は下書きのdirectoryで、(a) の規則がdirectoryごと消す。`capture_owner` の行は書かない。scratch directoryも、下書きのstoreの中（`sources/staging/`）に作り、下書きのdirectoryと一緒に消える。
- **引き渡し。** residentは、enqueueがrunの行をcommitした後に、その行を消す。enqueueは、既存の規則どおりsourceのguardを取ってからDBを操作し、commitまで持ち続ける（第23章）。取り込みを持つrunでは、そのguardの下で、`capture_owner` の行が `capturing` であることと、`sources/runs/<run>` があることを確かめ、どちらかが欠ければ `NOT_FOUND`（理由 `capture_abandoned`）で断って何も作らない。この確認は、runの行がまだ無いときにだけ行う（行があれば、引き渡しは済んでいる）。
- **持ち主の行を消すのはresidentだけである。** 取り込んだprocessは、自分の失敗（取り込みの失敗、enqueueの拒否、Ctrl-C）で終わるとき、平文の束（`sources/secrets/<run>.json`）をその場で消し（既存）、行の状態を `given_up` に変えてから終わる。行は消さない。残りは、削除の所有を持つresidentが消す。CLIは、sourceの削除の所有（第23章のBの排他）と親DBを持たないので、objectと親DBの行を自分では消せない。
- **放棄の判定。** 状態が `given_up` の取り込みと、持ち主のprocessが居ない（場所が同じで、pidが居ないか開始identityが違う）取り込みは、放棄されたものである。状態が `capturing` で持ち主のprocessが生きている取り込みは進行中で、何があっても消さない。場所が違う行は判定できないので残し、`doctor` が示す。場所・pid・開始identityと「居ない」の証明は、(a) の所有marker（`platform::owned_temp`）と同じものを使う。場所は、機械のidentity（macOSは `gethostuuid`、Linuxは `/etc/machine-id` だけ）・PID namespace・OSのbootの3つで、hostの名前は含まない。「場所が同じ」とは、機械のidentityとPID namespaceが同じことで、その上でOSのbootが違えば、持ち主はそのbootと一緒に終わっている。bootも同じなら、pidのprocessが居ないか、居てもその開始が記録より明らかに後なら（別のprocess）、持ち主は居ない。それ以外（読めない、zombie）は「居ない」としない。同じ名前のhostでも、機械のidentityが違えば場所は違う（bootが違い、pidが空いていても「居ない」としない）。行の場所は3行（`machine:<32桁>`、PID namespace、boot）で書く。1行目がこの形でない行（hostの名前を書いた公開前の版の行を含む）は、機械を言っていないので、場所が違う行と同じに扱う。residentが自分の機械のidentityを読めないときは、どの行も「居ない」としない。**機械のidentity（または自分の開始identity）を読めないprocessも、提出を断らない**（Linuxで `/etc/machine-id` が使えない環境、一部のcontainerやnotebook、macOSで `gethostuuid` が時間切れになる場合）。その行は、誰のものでもない場所（`unknown`）を持ち主として書く。この行は、どのreaderからも「居ない」と証明されず（場所が違う行と同じく判定できない扱い）、`given_up`、断られたenqueue、断られたcancelのどれかでだけ放棄される。そのprocessがkillされると、取り込み（pending root、object、平文のsecretの束、cancelの行）は、あとのどれかの明示の経路が来るまで残る。持ち主が居ないことを誰も証明できないので、残すのが唯一安全な扱いである（`doctor` が数える）。imageに機械ごとのmachine-idを持たせれば、通常の判定になる。
- **消す時点。** residentが、次の3つの出来事で、そのrunの放棄の処理を行う。(1) enqueueを断ったとき（その要求を処理するその場）。断ったresidentは、runの行が無いことを確かめた上で、行の状態を `given_up` にしてから放棄の処理を行う。持ち主のprocessが生きていて `capturing` のままでも、断られたenqueueがその取り込みの終わりである（CLIは、断られたrun IDでenqueueを送り直さない。送り直しても `capture_abandoned` で断られる）。residentが止まる途中で断ったときと、更新の試用中は行わず、(3) に任せる。(2) 行の無いrunへのcancelを、放棄を理由に断ったとき（下）。(3) 保持driverのpass（25.11）。passごとに `capture_owner` の行を全部見る（行の数は、進行中と放棄された取り込みの数である）。(3) はcrashと、要求を1つも送らずに終わったCLIの受け皿である。(1) と (2) の放棄の処理は、答えを書いた後に、同じ要求の中で行う（答えを書けなかったときも行う）。答えを待つ側の期限は5秒で、放棄の処理は、sourceのlockの待ち、親DBの削除、Vault、最大32件のclaimと内容の検証、unlinkごとのdirectoryのfsyncを含むからである。
- **放棄の処理。** sourceの削除の所有を取り、親DBにそのrunの行が無いことを確かめ直してから、下の「消すもの」を消す。runの行があれば、引き渡しは済んでいるので `capture_owner` の行だけを消す（持ち主の状態と場所に依らない）。sourceの削除の所有を待たずに取れなければ、何もせず、次のpassに任せる。
- **消すもの。** すべて、run IDと、行が持つpending rootのIDだけから名指せる。この順で消す。走査するdirectoryは、行が名指す取り込みのscratch directory（6）だけである。run IDが名指すfile（3・4・5・8）は、自分のuidの通常のfileで、link数が1のときだけunlinkし、symlinkをたどらない。そうでないものは残し、その段で止めて、`capture_owner` の行も残す（次のpassがやり直し、`doctor` が行を示す）。Vaultが使用中のときも、その段で止めて次のpassに回す。
  1. 親DBの、そのrun IDの `run_cancel_intent`・`control_request`・`event` の行。1つのtransactionで、runの行が無いことを確かめ直して消す。
  2. 親Vaultの `secrets/<run>.json`（第23章のCの手順。claimを先に書く）と、完了した `vault_delete_claim` の行。Vaultの一時fileは (a) の規則が消す。
  3. 平文の束 `sources/secrets/<run>.json`。
  4. `in-place/<run>.json`。
  5. `sources/runs/<run>`。
  6. 取り込みのscratch directory `sources/staging/<ulid>/` を、中身ごと（`<ulid>` は行のpending rootのIDから決まる。IDが `capture:<ULID>` の形でなければ、何も名指さずに止める）。開いたdirectoryから名前への操作だけで行い、symlinkをたどらない（linkそのものをunlinkする）。自分のuidでないentry、別のfilesystemにあるentry、名前がdirectoryでないscratch、取り込みがまだ `flock` を持っているscratchは残し、その段で止める。fileのlink数は問わない: 入れる途中で終わったfileは、storeの名前とscratchの名前の2つを持ち、ここで消すのはscratchの名前だけである。7より先に行う: 2つの名前を持つfileは、7の検証（link数が1）を通らない。無ければ成功である。件数の上限は置かない（commitの取り込みでは、checkout 1つ分のtreeになる。(a) の走査が `commit-*/` を消すのと同じで、sourceの排他lockの下で行う）。
  7. runのroot（`id = <run>`、`kind = 'run'`）とpending root（行のID）の、memberとgraph。他のrootが持たないobjectとmanifestのfile（下）。memberが空になったrootの行。そのrunの `import_session`・`run_expiry` の行。pending rootが `source` を持たない（登録が終わらなかった）とき、そのmemberは予約である。予約の宛先のfileが無いとき（名前か、その上のdirectoryが無い。それ以外の、確かめられない状態は「無い」としない）は、消すfileが無いので、memberの行だけを外す。
  8. `sources/runs/<run>.lock`（25.1の (A)）。
  9. 最後に `capture_owner` の行。
- **放棄されたrootのobject。** 今のBのpageは、期限の来たrunのrootを起点にするので、行の無いrunのrootと、どのrunにも結び付かなかったpending rootを訪れない。rootの行だけを消すと、objectのfileが残る。したがって、Bのpageの起点に「放棄された取り込みのrunのrootとpending root」を足し、そのmemberのうち他のrootが持たないobjectとmanifestを、Bの保護（claimを先に、所有するpathだけ、内容の検証、32件・64 MiB）のまま消す。rootの行は、memberが空になってから消す。1 passで終わらなければ、`capture_owner` の行を残して次のpassが続ける。放棄の処理は、objectを消す前に、2つのrootの種類を `abandoned` に変え、そのrootの未完了のgraphの行を消す。`abandoned` のrootは、「memberがまだ分からないroot（`source` がNULL）」として数えない（3の途中で終わった取り込みのpending rootは、そのままではBとCのpageとclaimの再生を止め続けるため）。それ以外の保護（古いmanifestの登録が済んでいること、ほかに `source` がNULLのrootと未完了のgraphが無いこと）は、Bと同じである。放棄の処理が書いたclaimには印を付け、crashで残った分は放棄のpassが再生する（出来事で消すものなので、時計のHoldを待たない。印の無いclaimは、今までどおりBのpageが再生する）。claimを書いた後のunlinkが失敗したときは、rootと `capture_owner` の行を残し、次のpassがそのclaimを再生してから続ける。正常な取り込みの段6（引き渡し）は、pending rootの行と一緒に、そのrootのgraphの行も消す（段階1より前は、graphの行が残った）。**memberになる前のfile**: 無い。取り込みは、storeへ入れる前に、入れるfileの全部をpending rootのmemberとして書く（上の「取り込みのscratch directoryと予約」）。どの時点で終わっても、この取り込みがstoreに作ったfileはmemberであり、それ以外に書いたものはscratch directoryの中にある。持ち主がkillされた後は、local-ciが返すはずだった「作った」「既に在った」の区別は残らないので、消すかどうかは、予約と「他のrootが持たないこと」だけで決まる。したがって、この取り込みが見つけて使っただけのfileでも、どのrootもmemberに持たなければ（予約をしない以前の版が残した、名指すものの無いfile）、放棄の処理が消す。他のrootが持つfileは消えない。予約のfileが無いと分かったときは、その予約を外し、同じtransactionで、memberもclaimも残らないobjectの `object` と `publication` の行も消す（どのpageも訪れない行を残さない）。
- **行の無いrunへのcancel。** residentは、runの行が無いrunへのcancelを、**そのrunの `capture_owner` の行が `capturing` で、場所が同じとき**に受理する。持ち主のprocessが生きていれば進行中の取り込みへのcancelで、持ち主が居なければ（`given_up` を書かずに終わった）、同じように書いた上で、答えの後に、その場で放棄の処理を行う。持ち主が居ない行のcancelも書くのは、enqueueが確かめるのが行の状態（`given_up` でないこと）だけで、持ち主が生きていることではないからである: 持ち主がenqueueを送った直後にSIGKILLされると、確認を通ったenqueueがguardを持ったままcommitへ進む。そこへ別のcommandのcancelが来て `NOT_FOUND` で断ると、放棄の処理はguardを排他で取れずに見送り、enqueueがcancelされていないrunをcommitする。書いておけば、commitされたrunはcancelを受け継ぐ。enqueueが来なければ、答えの後の放棄の処理（取れなければ次のpass）が、取り込みと一緒に3つの行を消す。residentが自分の場所（機械のidentity）を読めないときは、持ち主を判定できないので、`given_up` でない行はどれも進行中の取り込みとして同じように受理し（`cancel_control_of_capture`）、持ち主の生死で何も放棄しない。それ以外（行が無い、`given_up`、場所が違う）は `NOT_FOUND` で断り、`run_cancel_intent`・`event`・`control_request` のどれも書かない。行が `given_up` なら、断ったその場で放棄の処理を行う。確認と書き込みは、enqueueと同じくsourceのguard（共有）の下で行い、親DBのtransactionの中でrunの行の有無を確かめ直す（行があれば、行のあるrunへの通常のcancelになる）。断る前にも、runの行を確かめ直す: residentは、最初に親DBでrunの行が無いことを見てから、受け付けの排他を放して `capture_owner` の行を読むので、その間に、enqueueがrunの行をcommitして持ち主の行を消すことがある。このとき「行が無い」は「runが無い」を意味しない。したがって、`capturing` を読めなかったどの場合（行が無い、`given_up`、場所が違う、guardを取れない、読めない）も、受け付けの排他の下で、行のあるrunへの通常のcancelをもう1回試す。runの行があれば、それが答えである（受理。放棄の処理は行わない）。もう一度 `NOT_FOUND` のときは、持ち主の行を読めていれば（行が無い、`given_up`、場所が違う）、上のとおり断る。**持ち主の行を読めなかったときは、`NOT_FOUND` で断らない。** guardを時間内に取れなかったときは `SOURCE_BUSY`、それ以外の理由（sourceのrootの確認の失敗、索引の読み取りの失敗）は `COMMUNICATION_UNKNOWN` で答え、何も書かず、放棄の処理も行わない。読めない行は、runについて何も証明しない: 確認を通ったenqueueが、この後にcommitするかもしれない。提出の途中のCLIは、`NOT_FOUND` 以外の答えではcancelを200ミリ秒ごとに送り直す（上限は30秒。超えたら、結果は不明として77で終わる）。1回だけ送る `cancel <run>` は、その答えをそのまま返す（成功とは報告しない。`COMMUNICATION_UNKNOWN` の終了codeは77）。受理したcancelが書く `control_request` の行には、run IDを書く（下のschema）。
- **持ち主の行を書かない版が作った取り込み。** 最初の公開版が持ち主の行を書くので、書かない公開版は無い。公開前の版のCLIが残した取り込み（runの行も `capture_owner` の行も無い `sources/runs/<run>`）は、進行中か放棄かを証明できないので消さず、`doctor` が件数を示す。公開前の版が残した、行の無いrunの `run_cancel_intent`・`control_request`・`event` は、段階1のmigrationが消す（migrationは `coordinator.lock` の下で動き、その時点で進行中の取り込みのcancelを消しても、失われるのは「まだ提出していないrunのcancelの予約」だけである。そのCLIは、段階1のresidentに `capture_abandoned` で断られる。このmigrationは、更新の試用中にも動く。消すのは親DBの行だけで、fileは消さない。rollbackは親DBを退避から戻すので、25.1の8に反しない）。
- **期限。** 放棄された取り込みは、residentが断った要求があればその場で、無ければ、residentが動いていれば保持driverの次のpass（60秒以内）で、動いていなければ次のresidentの起動直後のpassで消える。

**挙動の変更。** (1) 行の無いrun IDへのcancelは、持ち主の行が `capturing` の取り込みのもの（進行中か、持ち主が `given_up` を書かずに終わったもの）を除いて `NOT_FOUND` になる（持ち主の行を読めなかったときは、`NOT_FOUND` ではなく、送り直しを求める答えになる）。今は受理して、`accepted = false`・`cancel_requested = true` の答えを返している。CLIの `cancel <run>` は、存在しないrun IDに `NOT_FOUND` を返す。(2) `storage/tests.rs` の `run_cancel_intent_precedes_enqueue_and_every_creation_and_place_gate` は、進行中の `capture_owner` の行を前提に加えた形に変える（cancelの予約がenqueueと配置の門より先に効くことは変えない）。行の無いrunへのcancelを前提なしで受理することを確かめている試験（`service/resident/tests.rs` の、提出前の `cancel_and_stop` の試験を含む）は、前提を足すか、`NOT_FOUND` を期待する形に変える。(3) 取り込みの途中のCtrl-Cは今までどおり受理される（持ち主の行が最初に書かれているので）。CLIは、提出が取り込みを始めた後のCtrl-Cでは、持ち主の行が在るかどうかを見ずに、必ずcancelを送る。行は「送るものが無い」ことの証拠にならない: residentはenqueueを受理すると行を消すので、答えを読む前にCtrl-Cを受けたCLIには、動いているrunの行が見えない。保持のpassがsourceのlockを排他で持っている間は、行を読めない（その間に、送ったenqueueがcommitされることがある）。行の無いrunへのcancelは `NOT_FOUND` で何も書かないので、余分に送っても害が無い。CLIは、cancelを送った後に、行が在れば `given_up` にして終わる。取り込みを始める前のCtrl-Cは、何も作っておらず、要求も送っていないので、cancelを送らずに終わる（結果の状態は `not_submitted`）。「始める前」は、process内の印で判る: 提出は、取り込みをblockingのthreadに渡す直前に、その場で印を付ける。(4) CLIは、run IDへのcancelに `NOT_FOUND` が返ったら、停止の確認を待たずに終わる（状態は `run_not_found`。止めるものが無い）。(5) sourceのlockを時間内に取れずに断ったenqueueの答え（`SOURCE_BUSY`）は、同じ要求の送り直しを求めない文にする（断られたenqueueは取り込みの終わりで、送り直しても `capture_abandoned` になる。やり直すのはcommandである）。

**安全の証明。** 取り込みの各時点でprocessが終わった場合に、残るものと、それを名指すものを挙げる。番号は「今の挙動」の順序で、0は持ち主の行の書き込みである。

| 終わった時点 | 残っているもの | 名指すもの |
| - | - | - |
| 0より前 | 何も無い | 該当なし |
| 0の後、2の前 | `capture_owner` の行 | 行そのもの |
| 2の後、3の前 | 加えて `sources/runs/<run>.lock` | run ID |
| 3の途中・後 | 加えてpending root、scratch directory（`sources/staging/<ulid>/`。commitの取り込みではcheckoutを含む）、予約（member）、storeへ入れたobjectとmanifestのfile、平文の束と `.secrets.tmp-*` | 行のpending rootのID（rootと同じtransactionで書く。scratch directoryの名前は、このIDから決まる）、run ID。`.secrets.tmp-*` は (a)。storeへ入れたfileは、入れる前に予約してあるので、どれもmemberである |
| 4の後 | 加えてrunのrootとそのmember | run ID（`root.id`） |
| 5の後 | 加えて `sources/runs/<run>` | run ID |
| 6の途中 | pending rootのmemberの一部が外れている。外れたmemberは、先にrunのrootへ写してある（`handoff_page` は、写した後に外す） | run IDとpending rootのID。両方のrootを起点にする |
| 6の後 | pending rootは無い。行のIDは、無いrootを指す（消す処理は「無い」を成功として進む） | run ID |
| 7の途中・後 | 加えてVaultの `secrets/<run>.json`（途中ならVaultの一時fileか、平文の束） | run ID。一時fileは (a) |
| 8の後 | 加えて `in-place/<run>.json` | run ID |
| 9を送った後、residentのcommitの前 | 同上 | 下の (2) |
| commitの後、行を消す前 | runの行と `capture_owner` の行 | 放棄の処理が、runの行を見て `capture_owner` の行だけを消す |

どの時点でも、残っているものは、`capture_owner` の行（run IDとpending rootのID）から名指せる。行より先に作られるものは無い。親のGit領域（`sources/git/<key>.git`）にlocal-ciが書いたobjectとrefは、この表の外である（25.16）。

(1) 進行中の取り込みは、状態が `capturing` で持ち主のprocessが生きている間は消されない。(2) 持ち主が終わった後に届くenqueue（CLIが送った直後に終わった場合）と放棄の処理は、sourceのguard（enqueueは共有、削除は排他）で直列になる。enqueueが先ならrunの行があり、放棄の処理は `capture_owner` の行だけを消す。放棄の処理が先ならenqueueは、行が無いことを見て断る。enqueueのcommitと、行の無いrunへのcancelが重なったときは、cancelがrunの行を確かめ直すので、commitされたrunへのcancelが `NOT_FOUND` になることは無い（上の「行の無いrunへのcancel」）。(3) 行の無いrunへのcancelと放棄の処理も、同じguardで直列になる。cancelが先なら、放棄の処理は消すものの1でその行を消す。放棄の処理が先なら、cancelは持ち主の行が無いことを見て断り、何も書かない。cancelを受理した後にenqueueが届けば、3つの行はrunのものになり、刈り取りが消す（25.5）。届かなければ、持ち主のprocessが終わるか `given_up` を書いた後に、放棄の処理が消す。持ち主が居ない `capturing` の行へのcancelも同じで、書いたcancelは、guardを持っているenqueueがcommitすればrunのものになり、そうでなければ答えの後の放棄の処理が消す（その後に届くenqueueは、行が無いことを見て断られる）。したがって、行の無いrunのcancelの行で、消すものが決まっていないものは無い。(4) `sources/runs/<run>.lock` を開くのは、そのrunを取り込むprocessだけで（`lock_run` の呼び出しは取り込みの1か所）、lockより先に持ち主の行があり、そのprocessは居ないか、`given_up` を書いて取り込みを終えている。run IDは使い回さない。(5) 起動時の走査は、`sources/runs/` と `in-place/` のentryを「行の無いrunのもの」として消すことをしない（25.11）。これらを消すのは、`capture_owner` の行を起点にしたこの規則と、刈り取りだけである。

**受け入れ試験。** (i) 上の表の各時点（0の後、2の後、3の途中、4の後、5の後、6の途中、6の後、7の後、8の後）でCLIをSIGKILLし、次のpassの後に、`sources/retention.db` のそのrunの行（`capture_owner`・`root`・`member`・`graph`・`import_session`）、他のrootが持たないobjectとmanifest、`sources/runs/<run>` とlock、平文の束、Vaultの束、in-placeのbindingが、どれも残らない（時点ごとに1つ。fileと行を列挙して確かめる）。(ii) 取り込み中（持ち主が生きている）にresidentを再起動しても何も消えず、続くenqueueが成功する。(iii) CLIがenqueueを送った直後に終わり、放棄の処理と競っても、「runが受理されて全部残る」か「断られて全部消える」のどちらかになる。(iv) 提出が断られたとき、断ったその場で全部消える（次のpassを待たない）。(v) 場所が違う行は消えず、`doctor` に出る。(vi) 放棄された取り込みのobjectのfileが、他のrootが持たなければ消え、持てば残る。(vii) 持ち主の行の無い `sources/runs/<run>` は消えず、`doctor` に出る。(viii) `capture_owner` の行が無いrun IDへのcancelが `NOT_FOUND` で、親DBの行が1つも増えない。(ix) 取り込み中のcancelは受理され、その後CLIをSIGKILLすると、次のpassで `run_cancel_intent`・`control_request`・`event` のそのrunの行が残らない。(x) 取り込み中のcancelの後にenqueueが届くと、今までどおりcancel済みのrunになる。(xi) cancelの受理と放棄の処理を競わせ、「受理されて、後の放棄の処理が行を消す」か「`NOT_FOUND` で何も書かれない」のどちらかになる。(xii) `given_up` を書いたCLIからのcancelは `NOT_FOUND` で、その場で放棄の処理が行われる。(xiii) 計画の下書きの取り込みが `capture_owner` の行を書かない。(xiv) 行の無いrunへのcancelが持ち主の行を読む前（と、放棄を読んだ後、guardを取れなかった後）にenqueueがcommitされると、cancelは受理され、runはcancel済みになる。(xv) 放棄を理由に断ったcancelと、断ったenqueueの答えが、放棄の処理の始まりより先に読める。(xvi) enqueueが受理されて持ち主の行が消えた後と、sourceのlockが排他で持たれている間の、提出の途中のCtrl-Cが、cancelを送る。(xvii) 行がpending rootを名指している間、同じrunの2つ目のpending rootが断られ、先のrootが放棄の処理で消える。(xviii) 放棄の処理が1 passで終えられなかった `abandoned` のrootが、Bのpage・Vaultのpage・previewを止めない。(xix) `submit` と `ci run` の、取り込みを始めた後の失敗が `given_up` を書く（持ち主が生きているので、ほかに消せる理由が無い）。(xx) runの行があるrunのeventと、run IDの無いeventの削除が断られる。 (xxi) 機械のidentityだけが違う場所（hostの名前は同じ。bootが別で、pidが空いていても）の持ち主は「居ない」とされず、hostの名前を書いた行は場所が違う行として残る。 (xxii) 持ち主の行を読めないcancelの答えは `NOT_FOUND` でなく、何も書かれず、確認を通っていたenqueueがcommitした後の次のcancelがrunをcancelする。 (xxiii) `given_up` を書いた後と、行が消えた後の取り込みが、pending rootを作らない。 (xxiv) 持ち主がSIGKILLされ、確認を通ったenqueueがcommitする前に届いたcancelが記録され、commitされたrunがcancel済みになる。enqueueが来なければ、同じ要求の中の放棄の処理が、取り込みとcancelの行を消す。 (xxv) 取り込みを、scratch directoryへ書き終えて予約する前・予約した後で入れる前・入れた後で登録する前、の3つの時点でSIGKILLすると、順に「scratch directoryだけ」「加えて予約」「加えてstoreのfile」が在り、次のpassの後には、scratch directoryも、予約も、storeのfileも残らない。 (xxvi) 入れた後にkillされた取り込みのfileのうち、他のrootが持つもの（見つけて使っただけのもの）は残り、持たないものだけが消える。 (xxvii) 入れる途中で終わって、scratchの名前が残ったfile（link数2）が、scratch directoryを消した後に消える。 (xxviii) 入力が動き続けて `SOURCE_BUSY` で終わった取り込みが、storeにfileも予約も残さず、scratch directoryも残さない。 (xxix) 宛先に別の内容のfileがあって途中で止まった取り込みが、作ったfileの予約だけを残し、放棄の処理がそれだけを消して終わり、在ったfileは変わらない。 (xxx) 入れた後にkillされた取り込みの放棄の処理を、scratch directoryを消す前後・claimの後・unlinkの前後・rootの後で止めても、後のpassが終える。 (xxxi) scratch directoryの中のsymlinkはたどらずに消え、指す先は残る。scratchの名前そのものがsymlinkのときと、放棄の処理から見て `flock` が持たれている間は、何も消さない。pending rootのIDが `capture:<ULID>` の形でなければ、directoryを作らず、消さない。自分のuidでないentryと、別のfilesystemのentryを「自分のもの」としない（`lstat` の値を与える関数で確かめる）。 (xxxii) scratch directoryを消せなかった取り込みは失敗し、放棄の処理は消せるようになるまで行を残し、その後のpassが消す。 (xxxiii) commitの取り込みのcheckoutがscratch directoryの中に在り（`sources/tmp/` には無い）、killの後、scratch directoryと一緒に消える。 (xxxiv) 予約は、`source` を持たないpending rootにだけ書け、digestやsource IDの形でないもの・索引の決めるpathでない宛先・削除のclaimが付いたobjectが1つでもあれば、何も書かない。sourceで名付けた束は予約に入らない。 (xxxv) fileが無いことでmemberを外すのは予約だけで、登録の済んだrootのmemberは、fileが無くても残る。確かめられない場所（storeのdirectoryの位置にあるsymlink）の予約も残る。

**schema。** `sources/retention.db`: `capture_owner(run_id PRIMARY KEY, state, root, place, pid, start)`（版は上げない。上の規則）。`root.kind` の値 `abandoned`。`source_delete_claim` のclaimの印（JSONの中。古い版は読み飛ばす）。取り込みの予約とscratch directoryのために、表も列も足さない（予約は `member` の行で、scratch directoryの名前は `capture_owner.root` と `root.id` から決まる）。親DB: `control_request.run_id` と索引（cancelとretryの受理が対象のrunを書く。既存の行は、migrationが `target_json` から埋める）、索引 `event(run_id, seq)`、`event_no_delete` を、条件つきのtriggerに置き換える: `run_id` がNULLのeventと、runの行があるrunのeventの削除は今までどおり断り、行の無いrunのeventの削除だけを通す（段階1でeventを消すのは、放棄の処理とこのmigrationだけで、どちらも行の無いrunのeventである）。migrationは、runの行が無いrun IDの `run_cancel_intent`・`control_request`・`event` を消す。

### (c) `in-place/<run>.json`

**段階1より前の挙動。** 提出が断られたときだけ消す。通常のrunのものは消さない。

**規則。** 持ち主はrunである。消すのは、提出が断られたとき（既存）、取り込みが放棄されたとき（上の (b)）、runの刈り取り（段階2）である。段階1で入るのは前の2つである。

### (d) 一時workerの起動し直し

**この規則より前の挙動。** 一時worker（第15章）のstate（`~/.belt`）は、liveなattemptが無い状態で正常に終わったときだけ消えた。SIGKILLや電源断で残ると、次の `belt worker --ephemeral` は起動を断り、人が消すまで使えなかった。下の規則は実装済みである（受け入れ試験の (vi)・(vii) はS26のDocker環境で確かめる）。

**規則。** 残ったstateを、次の一時workerが自分で安全に片付ける。

- **所有marker。** 一時workerは、stateのdirectoryを作ったら、まず `ephemeral.lock` を新しく作って（既にあるfileは使わない）共有のflockを取り、その後で `ephemeral.json` を書く。隣の `ephemeral.json.part` に書いてからrenameするので、`ephemeral.json` は完成した形でだけ現れる。この順にする理由は、「読める `ephemeral.json` があるなら、書いた起動はそれより前から `ephemeral.lock` を持っている」を成り立たせるためである。markerを先に書くと、markerはあるがlockをまだ持っていない起動のstateを、片付ける起動が「誰も使っていない」と読み、lockとmarkerを消した後で、その起動がmarkerの無いstateで動き続ける。directoryを作ってからmarkerが現れるまでの間にkillされた起動は、markerの無いstate（空か、`ephemeral.lock` と `ephemeral.json.part` だけ）を残す。これは下の「作りかけのstate」の規則で、次の起動が消す（人の手は要らない）。作成が失敗したときは、自分が作った名前だけを、lockを持ったまま消す（markerを先に。自分が作っていない名前には触れない）。中身は、種類（一時worker）、OSのboot（`platform::boot_id`。Linuxは `/proc/sys/kernel/random/boot_id`、macOSは `kern.bootsessionuuid`。起動ごとの乱数なので、別のhostと偶然そろわない）、pid、processの開始identity、instance、書いた起動が自分で見たfilesystemの種類とその判定（`filesystem` の `kind` と `local`）である。hostnameとPID namespaceの番号は、hostの識別には使わない（hostnameは共有volumeの2台で同じになり得る。PID namespaceの番号はkernelごとの連番で、hostをまたいでそろい得る）。
- **生存のlock。** 一時workerは `ephemeral.lock` を開いて共有のflockを取り、そのdescriptorを、自分が起動するすべてのprocess（supervisor、setup builder、利用者のcommand）へ**継承させる**（close-on-execにしない）。flockは開いたfileの記述に付くので、継承したprocessが1つでも生きている間、lockは持たれたままになる。
- **ローカルのfilesystemに限る。** stateのdirectoryがローカルのfilesystem（`statfs` の種類が許可の一覧にあるもの。APFS・HFS+・ext4・xfs・btrfs・zfs・tmpfs・overlayfs）にあるときだけ、自分で片付ける。network filesystemと、hostとguestで共有するfilesystem（NFS・SMB・9p・virtiofs・FUSE）では、flockが他のkernelのprocessを排他する保証が無いので、従来どおり起動を断る。一覧に無い種類も断る。判定は2つ要る。stateを作った起動が、自分で見た種類と判定を `ephemeral.json` に記録する。片付ける起動は、その記録と、自分が今見ている判定の両方がローカルのときだけ片付ける。同じdirectoryが、中で動くworkerには共有のfilesystemに見え（containerがhostのdirectoryをbind mountしたもの。Docker Desktopではvirtiofs）、hostの側の起動にはローカル（APFS）に見えることがあり、片側のflockがもう片側に届く保証が無いからである（NFSのserver自身とclientも同じ）。記録の無いmarkerでは片付けない。一覧に無いfilesystemでstateを作ること自体は断らない（断るのは片付けだけ）。
- **片付けてよい証明。** 次のすべてが成り立つとき。(1) `ephemeral.json` が読めて、種類が一時workerである。lockを取った後に読み直し、調べたときと同じfile（inode）で同じ中身であること（lockを持つ前に読んだmarkerは、証明に数えない）。(2) stateがローカルのfilesystemにある（作った起動の記録と、今の判定の両方）。(3) 常駐workerのjournalがこのhostに無い。見るのは、workerのserviceの `worker-service.json` と、`init` のresidentの `init.json`（同じ設定directoryにあり、同じstateでworkerを動かす）の2つである。「無い」と言えるのは「見つからない」のときだけで、調べられないとき（権限が無い、途中がdirectoryでない）は、そのerrorを示して断る。(4) `agent.lock` を取れた。(5) `ephemeral.lock` を排他で、待たずに取れた。
- **片付けは1つずつ。** (1)〜(3) を読んだ後、stateのdirectoryそのものに、排他のflockを待たずに取る（directoryのdescriptorへのflockで、fileは作らない）。取れなければ断る。2つの起動が同時に片付けると、一方が `ephemeral.lock` をunlinkした後に、もう一方が新しい `ephemeral.lock` を作って取り、(1) の読み直しで断って、lockだけのdirectoryが残るからである。markerの無いstateでは、下の「作りかけのstate」の条件（名前・filesystem・journal）を読んだ後にだけ取る。
- **作りかけのstate。** markerの無いstateのうち、作成の途中でkillされた起動が残したものは、次の起動が自分で消す。次のすべてが成り立つとき。(a) directoryの中身が `ephemeral.lock` と `ephemeral.json.part` の2つ以内で、`ephemeral.json` が無い（空でもよい。他の名前が1つでもあれば、従来どおり何も書かずに断る）。(b) 今の判定でローカルのfilesystemである（markerが無いので、作った起動の記録は無い。ローカルでなければ、別のhostの起動が作成の途中かもしれず、ここのlockでは分からないので断る）。(c) 常駐workerのjournalが無い（(3) と同じ2つ。directoryを開いた後に読む）。ここまでは何も書かず、何もlockしない。成り立てば、stateのdirectoryに排他のflockを取り、`ephemeral.lock` を開いて（無ければ新しく作って）排他で、待たずに取り、**lockを持ったまま** (a)〜(c) を読み直す。さらに、持っているlockが今その名前にあるfile（同じinode）で、pathがまだ調べたdirectoryであることを確かめる。消すのは `ephemeral.json.part` と `ephemeral.lock` の2つの名前だけで（descriptorからの相対）、最後に親directoryのdescriptorから、再帰しない `rmdir` をする。証明の後に現れたものは消えず、`rmdir` が失敗してdirectoryごと残る。断るときは、自分が作って持っている `ephemeral.lock` だけを消して、他は元のままにする。この形になるのは3つで、どれも同じ規則で消える。作成がdirectoryの後・markerのrenameの前にkillされた。失敗した作成が、自分の作ったものを消している途中でkillされた。片付けが、最後のunlinkと `rmdir` の間でkillされた（空のdirectory）。
- **作る側との競合。** 作りかけを消す起動は、作成の途中の起動のdirectoryを、残り物と見て消しに来ることがある。作る側は次のように引く。`ephemeral.lock` は必ず新しく作る（`O_EXCL`）。既にあれば、消す側が作ったものなので、何にも触れずに失敗する。作った直後の共有のflockが取れなければ、消す側が持っているので、作ったlockにも触れずに失敗する（lockを持たずに名前でunlinkすると、自分のfileを消したと言えない）。どちらの場合も、directoryは消す側が消す（消す側もkillされたら、その次の起動が消す）。作る側が共有のflockを持った後は、消す側は排他で取れずに断る。消す側がdirectoryを消した後に作る側が `ephemeral.lock` を作ろうとすると、消えたdirectoryの中なので失敗する。作る側は失敗して1で終わり、利用者はもう1回起動する。
- **空のdirectoryを作る他の経路。** `init` は `init.json` を書いてからworkerのstateのdirectoryを作るので、(c) を「directoryを開いた後に読む」ことで除ける（directoryが見えたなら、journalは既にある）。journalより前に最初のlock fileを作る経路（常駐の `belt worker` を手で起動したときなど）は、次のどれかになり、どれも失うものが無い。そのfileがlockの下の読み直しより前に現れる（(a) が成り立たず断る）。読み直しと `rmdir` の間に現れる（`rmdir` が失敗し、directoryは残る）。`rmdir` の後に作ろうとする（「見つからない」で失敗し、やり直せば通る）。
- (5) は「前の起動が始めたprocessが、このkernelのどのPID namespaceにも残っていない」を示す。別のhostで生きているworkerのstateである可能性は (2) で除く。
- **消す。** 証明が成り立てば、stateのdirectoryを消し、新しいstateを作って、新しいidentityで起動する。消すものは、調べたdirectoryのdescriptorからの相対（`openat`・`unlinkat`）でだけたどり、symlinkはたどらない。pathは使わないので、証明の後でpathが別のもの（symlinkなど）に差し替わっても、その先は消えない。順番は、data、`worker-owner.lock`、`agent.lock`、`ephemeral.lock`、markerで、markerを最後にする（途中でkillされた片付けを、次の起動が証明し直せる。markerより長く残り得る名前は `ephemeral.lock` だけで、それは「作りかけのstate」の形である）。最後に、親directoryのdescriptorから、名前がまだ調べたdirectory（同じinode）であることを確かめて `rmdir` する。違っていれば、そこにあるものには触れずに1で終わる。成り立たなければ、何も消さずに1で終わり、成り立たなかった条件を示す。processにsignalは送らない（前の起動のprocessを止めるのは、利用者か、containerの終了である）。
- **mountの境界。** stateの中に別のfilesystemがmountされていることがある（attemptのworkspaceの中で生きているFUSEやbind mount。Colabでは一時workerがrootで動く）。消す処理は、`st_dev` がstateのdirectoryと違う子には入らず、unlinkもしない（見た名前の `lstat` と、開いたdescriptorの `fstat` の両方で比べる。fileのmount pointも同じ）。その子と、そこまでの親directoryは残り、lockのfileと `ephemeral.json` も消さない。起動は、残った場所を示して1で終わる。mountが外れれば、次の起動が証明し直して残りを消す（mountを外すのは人である。mountの先を消してよい証明は無い）。限界: 同じfilesystemの別のdirectoryをbind mountしたものは `st_dev` が同じで、区別できない。
- **containerの再起動。** PID namespaceが新しくなり、前のnamespaceのprocessはすべて終わっているので、(5) が成り立ち、自分で片付く。同じvolumeを2つのcontainerが同時に使う場合は、もう一方が生きている間 (4) か (5) が成り立たず、断る。
- **OSの再起動。** lockは誰も持っていないので成り立つ。markerのbootは診断のために示す。
- **lockを継承しないprocess（(5) の限界）。** (5) が示すのは「lockのdescriptorを継承したprocessが残っていない」までである。継承しないprocessは、`ephemeral.lock` が空いて見える間もstateの中のfile（workspace）を使っていることがあり、そのとき次の起動は、使用中のworkspaceを消す。継承しないのは2種類である。1つは、別の常駐processに起動させたもの（dockerdが起動するcontainer（`docker run`・composeのservice）、launchd、`systemd-run`、既にあるtmux serverの中のcommand）。もう1つは、継承したdescriptorを閉じるもの（Pythonの `subprocess` は既定で `close_fds=True`、sudo、`closefrom` を呼ぶprogram、全descriptorを閉じて常駐化するcommand）。これらは、attemptの停止の規則（第12章）でも追えないprocessであり、正常終了の経路でも同じである。workspaceをcontainerへbind mountするcommandを動かしていたhostでは、起動し直す前にcontainerを止める。逆に、commandが起動した別の常駐process（build daemonなど）がdescriptorを継承して生きていれば、(5) は成り立たず、断る（安全の側）。
- **非一時の `belt worker`。** stateに `ephemeral.json` があるとき（作成の途中で終わった `ephemeral.lock`・`ephemeral.json.part` だけのときも同じ。常駐workerはこの名前を作らない）、常駐workerは起動を断り、stateに触れない。断るのは、`belt worker` の起動、`init`（新規も、導入済みのworkerの引き継ぎも）、`init` のresident（同じstateでworkerを動かす）の起動の準備、`worker install`、`worker start` で、どれも最初の書き込みより前に確かめ、同じ文で断る。`init` は、設定directoryにも何も書く前に断る（`init.json` が残ると、(3) によって、その後の一時workerがこのstateを片付けられなくなる）。一時workerのidentityとattemptを、常駐workerが引き継ぐことはしない。利用者は `belt worker --ephemeral` を1回起動するか、directoryを消す。
- **正常終了。** liveなattemptが無ければstateを消す、という条件は変えない。消し方は片付けと同じにする。stateのdirectoryに排他のflockを取り（他の起動が同時に調べたり片付けたりしない。pathで丸ごと消すと、`ephemeral.lock` がmarkerより先に消え、別の起動がmarkerを読んで新しいlockを作り、同時に片付け得る）、`worker-owner.lock` と `agent.lock` を排他で取り（このstateで動くworkerが居ない）、その後で2つのjournalを読み直す。一時workerが動いている間に常駐workerが導入されていたら（journalがある。調べられないときも同じ）、そのfileが同じdirectoryにあり得るので、何も消さずに、その旨をstderrに示して1で終わる。消すのは、上の「消す」と同じ手順である（descriptorからの相対、mountの境界、markerを最後、inodeを確かめてから再帰しない `rmdir`）。自分が共有で持っている `ephemeral.lock` を排他に取り直すことはしない（directoryのflockが片付けを締め出している。取り直すと、継承した子が生きているだけで正常終了がstateを残すようになる）。

**安全の証明。** stateを読める・書けるのは、前の起動のagent（`agent.lock`）、supervisorとcommand（`ephemeral.lock` の継承）、別のhostやguestのprocess（作った側と片付ける側の両方でローカルのfilesystemに限ることで除く）、常駐worker（2つのjournalで除く）、作成の途中の一時worker（markerより先にlockを持つので、markerが読めるなら `ephemeral.lock` で分かる。markerが無い間は、作りかけを消す起動と `ephemeral.lock` の作成・flockで順序が付き、負けた側は何にも触れない）である。どれも居ないことを確かめてから消す。作りかけのstateには、消して困るdataが無い（消す名前は、作成がmarkerより前に作る2つだけで、他の名前があれば消さない）。lockを継承しないprocessは、この証明の外にある（上の限界）。古いidentityのattemptは、親の側ではunknownのまま残り、`workers forget` で確定する（第15章）。新しい起動は新しいidentityなので、消したstateの記録を必要とする相手は居ない。

**受け入れ試験。** (i) agentをSIGKILLし、commandも終わった後の再起動が、stateを消して新しいidentityで起動する。(ii) commandが生きている間の再起動は断られ、何も消えず、commandにsignalが届かない。(iii) markerが無く、作りかけの2つ以外の名前があるstate、種類の違うmarkerでは断る。(iv) 許可の一覧に無いfilesystemでは断る（種類の判定を差し替えて確かめる）。(v) 常駐workerが `ephemeral.json` のあるstateで起動を断る。(vi) S26のDocker環境で、containerを `kill` して再起動すると自分で片付く。(vii) 同じvolumeを使う2つ目のcontainerは断られる。(viii) 作った起動が共有のfilesystemと記録したstate、記録の無いmarkerのstateは、今ローカルに見えても断る。(ix) 作成のどの時点でもmarkerは無く、lockを持った後に片付けが来れば、lockが持たれていて何も変えずに断る。調べている間にmarkerが変わったら断る。持っているlockが、今その名前にあるfileでなければ断る。(x) 証明の後でpathをsymlinkに差し替えても、その先は消えない。(xi) `init.json` があるhostと、journalを調べられないhostでは断る。(xii) `worker install`・`worker start`・`init` のresidentが、`ephemeral.json` のあるstateに何も書かずに断る。(xiii) 別の起動が片付けている間（directoryのflock）は断る。(xiv) 作りかけの形（空、`ephemeral.lock` だけ、`ephemeral.lock` と `ephemeral.json.part`、`ephemeral.json.part` だけ）は、次の起動が消す。journalがある、ローカルでない、lockが持たれている、他の名前がある、のどれかなら、何も書かずに断る。lockの下の読み直しで変わっていたら、自分が作ったlockだけを消して断る。証明の後に現れたものは消えない。(xv) 作る側は、`ephemeral.lock` が既にあるとき、作ったlockを共有で取れないとき、何にも触れずに失敗する。失敗した作成は、自分が作った名前だけを消す。(xvi) stateの中の別のfilesystem（`st_dev` を差し替えて確かめる）には入らず、その親とmarkerを残して断り、外れた後の起動が消す。(xvii) markerのあるstateへの `init`（新規・引き継ぎ）は、何も書かずに同じ文で断る。(xviii) 正常終了は、directoryのflockの下で、markerを最後に消す。journalが現れていた、`worker-owner.lock` か `agent.lock` が持たれている、別の起動が調べている、のどれかなら何も消さない。(xix) lockを継承した子のprocessが生きている間は、作った起動が居なくなっても片付けを断る。

**schema・wire。** `ephemeral.json`（`filesystem` に、作った起動が見た種類 `kind` と判定 `local`）、書く途中だけある `ephemeral.json.part`、`ephemeral.lock`（どれもstateと一緒に消える。lockは固定のfileで、state1つに1個）。片付けの直列化はstateのdirectoryへのflockで、fileは増えない。wireの変更は無い。

## 25.5 段階2: runの刈り取り

**刈り取り**は、投げたメンバーが1つのrunを自分のディスクから完全に消す処理である。

**今の挙動（段階3を入れた後）。** 親は、下の8段のうち、段4の親のGit領域のrefの削除（local-ciに関数が無い。25.16）を除いて、この節のとおりに消す。段階2より前は、runの行とevent、`reader-pins/<run>.lock`、`sources/runs/<run>` とそのlock、`sources/retention.db` の行、通常のrunの `in-place/<run>.json` を消さなかった。結果・入力・Vaultは、刈り取りより前に、それぞれの保持driverが消している（第23章）。刈り取りは、その残りを消す。

**条件。** runが終わっていて（25.3の述語）、終点から `retention_days`（既定7日）が過ぎていること。`observation` がunknownのattemptを持つrunは、述語の3で条件を満たさない。時計がHoldの間、試用中は行わない。時計は、結果のGCの時計（親DBの `result_gc_clock`）である。

印は取り消せないので、後の段が求めるものは、すべて印の条件に含める。次の2つを、述語に足して確かめる。(1) taskを1つも持たないrunは、閉じている（`run.closed_at_ms` がある）ときだけ終わっているとし、終点は閉じた時刻とする。閉じていないrunは、最初のtaskがこれから作られ得る（受理の直後のrunを、時刻0に終わったrunと読まない）。入力の保持の終点（25.3の (a)）は、taskの無いrunに終点を返さないままである（そのrunの入力は、刈り取りの段4が消す）。(2) retireされていないattemptのterminationが、この版で型として読めること（入力の保持が `input_retention_binding` で確かめているのと同じ検査）。読めない記録を持つrunは、印を付けずに全体を残す。印の後の段は、保持期間・ACK・時計を確かめ直さない。

**7日の読み手。** `show`・`logs`・`outputs`・明示 `retry`・GitHub statusの再投稿である。どれもrunの行と、結果または入力を一緒に必要とする。入力が消えたrunはretryできず（第6章）、結果が消えたrunは `logs`・`outputs` が読めない。行だけを結果より長く残す読み手は無いので、行も同じ時点で消す。runの行を読んで学習する処理（見積、forecast、testの時間）は無い（それぞれ別の表を読む）。

**順序。** 各段は、途中で落ちても次のpassが同じ段からやり直せる。再開の手掛かりは `run.pruning_at_ms` で、印のあるrunは起動直後のpassが最初に処理する。

| 段 | 行うこと | 途中で落ちたとき |
| - | - | - |
| 1 | `run.pruning_at_ms` を書く。同じtransactionで条件を確かめる | 印が無ければ何も起きていない。あれば2から |
| 2 | 親Vaultのsecret bundleを消す（第23章のC。通常は、runが終わった時点で保持期間を待たずに消してある） | Cのclaimの再実行 |
| 3 | runの排他pinを、待たずに取る。取れなければ（readerが読んでいる）このrunを次のpassに回す。pinのfileが無いrunは、読み手が居ないものとして、pinなしで進む（readerはfileを作らず、作る側は印の無いrunにだけ作る。attemptを名指す関数は、断られたときにpinのfileを消し得る）。取れたら、結果（`results/<attempt>/` のresult・log・artifact・`tmp/`）を、既存の結果のGCの手順（参照を外してclaimを書く、fileを消す、claimを消す）で全部消す。印のあるrunでは、保持期間・ACK・回収の状態の判定を省く（条件で済んでいる）。結果のstoreのものでないlocatorの参照は、参照だけを外す（fileはbeltのものでない）。参照が無くなった後、そのrunの各attemptの `results/<attempt>/` を消す（参照を持たないattemptに、途中で終わった転送の `tmp/` だけが残っている場合がある） | 削除claimの再実行（既存）。`result_delete_claim` と `pending_delete` の行は、fileを消した後に消える |
| 4 | 取り込みの持ち主の行が保留のrootを名指していれば、その取り込みのscratch directory（`sources/staging/<ulid>/`）を先にまるごと消す（放棄の処理の段6と同じ）。入力の所属を外し、他のrunが参照しないmanifestとobjectを消す（第23章のB）。そのsourceを参照するrunが他に無ければ、そのsourceのbundle（`sources/bundles/<source>-<32hex>.bundle`）と、親のGit領域のそのsourceのref（下） | Bのclaimの再実行。bundleとrefの削除は冪等 |
| 5 | runごとのfileと索引の行: `in-place/<run>.json`、`sources/runs/<run>`、`sources/retention.db` のそのrunの行（`run_expiry`、完了した `vault_delete_claim`、そのrunのrootとそのmember・graph、`import_session`、`capture_owner`）、最後に `sources/runs/<run>.lock` | 各削除は冪等。未完了のclaimが残るrunは、ここで止めて次のpassに回す |
| 6 | 親DBの行を、下の順で消す。1 transactionは1,000行まで | 印のあるrunの残りの行を、同じ順で続ける |
| 7 | 最後のtransaction（`run` の行を消すもの）のcommitの後、そのrunのattemptのうち、workerが受理していて、ACK済みでないものに `release_attempt` を送る（届かなくてよい。25.6）。実装は、runのattemptを全部送る（workerは、受理していないものとACK済みのものに `NOT_FOUND` と答える）。送る相手の一覧（workerとattempt ID）は、段6でattemptの行を消すtransactionが、消す前に読んでメモリに持つ | 一覧は失われる。送らなかった分は、接続時の照合（25.6）が拾う |
| 8 | `reader-pins/<run>.lock` をunlinkし、排他pinを手放す（25.1の (B)）。結果のGCのcursorがこのrunを指していれば、cursorを捨てる（下） | 行が無くpinだけが残る。起動時の走査が消す（25.11） |

**1 passの上限。** runは32件（印のあるrunを先に、run IDの順で。進めないrunが後ろのrunを待たせないように、続きの位置をメモリに持つ）、結果のpathは32件（段3のclaimと、attemptごとのdirectory `results/<attempt>/` の削除を1件ずつ数える）、入力のpathは32件と検証64 MiB（段階1の放棄の処理と同じ関数の上限）、行は1,000行のtransactionを64回までである。どの上限も、1 passの全runで共有する（runごとに取り直さない）。上限に達したrunは、印を持ったまま次のpassが続ける。attemptのdirectoryの走査は、続きの位置（task IDとattempt IDの順）をメモリに持ち、次のpassがそこから続ける（失っても、走査をやり直すだけで、消してよいものは変わらない）。印のあるrunは、部分索引 `run(run_id) WHERE pruning_at_ms IS NOT NULL` で探す（全runを読まない）。印のあるrunの数は `doctor` が示す。段4が入力の保持のgate（旧形式のmanifestの登録が済んでいない、進行中の取り込みがある、graphが終わっていない）で止まっている間は、`doctor` の行と、residentのログの `run pruning held` の行が、そのgateの名前（`legacy_registration`・`capture`・`graph`）を示す。

**段4・段5の実装。** 段階1の「放棄された取り込み」の処理（25.4の (b)）と同じ関数を使う: sourceの変更lockを待たずに排他で取り、親DBでrunに印があることを確かめ直してから、runのroot（と、取り込みの持ち主の行が名指すpending root）の所属を外し、他のrootが持たないfileを、証明・claim・unlinkの順で消す。したがって、段4は第23章のBのpage（時計と `input_retention_binding` を確かめる）を経ずに、印を根拠に消す。証明できないfile、進行中の取り込み（所属の分からないroot）がある間は、そのrunをそこで止めて次のpassに回す。段2のVaultも、同じく放棄の処理の関数（Vaultのclaim・unlink・完了）を使う。

**passをまたぐpin。** 排他pinは、段3から同じpassの終わりまで持ち、passの終わりに手放す。passをまたいでは持たない。次のpassは取り直す。pinを持たない間も、印がすべての読み手を断る（25.3）ので、半分消えたrunは見えない。印の前に共有pinを取って読み始めたreaderは、排他pinを取れない間、刈り取りを段3で待たせる。

**結果のGCが、消したpinを作り直さない。** 今の結果のGC（`src/results/retention.rs` の `page`）は、処理の途中のrunをcursor（`cursor.current`）に持ってpageをまたぎ、pageの最初に `result_gc_guard`（`src/storage/coordinator/retention.rs`）でpinを取る。`result_gc_guard` は、pinのfileを無条件に作って（`RunPin::ensure`）から排他で取る。したがって、cursorが指すrunを刈り取りが消すと、次のpageが、段8でunlinkしたpinを作り直し、行の無いrunのpinが残る。次の2つを規則とする。

- `result_gc_guard` は、1つの読み取りtransactionで、そのrunの行があり、印が無いことを確かめたときだけ、pinを作って取る。行が無いか印があれば、pinに触れずに「このrunは無い」と答え、GCはそのrunのcursorを捨てて次のrunへ進む。**規則として**、結果のGCのpageと刈り取りは、residentの同じ保持driverの中で、重ならずに動かす（25.11）。したがって、確認とpinの作成の間に、刈り取りが段8まで進むことは無い。
- 刈り取りは、段3でこのrunの結果を消すとき、結果のGCのcursorがこのrunを指していれば捨てる。段8でも確かめる。cursorはresidentのメモリにあり、再起動で空になる。

pinのfileを作る（`RunPin::ensure` を呼ぶ）のは、次の3つがすべてである。(1) `result_gc_guard`（上）。(2) runを受理する経路（`src/storage/coordinator/queue.rs`。新しいrun IDについてだけ呼ぶ）。(3) residentが、結果を書くためにattemptを名指して共有pinを取る関数（`src/storage/coordinator.rs` の `result_writer_pin`。attemptの行からtaskとrunをたどり、`ensure` してから共有で取る）。(3) にも (1) と同じ規則を当てる: 1つの読み取りtransactionで、attempt・task・runの行があり、runに印が無いことを確かめたときだけ作る。行が無いか印があれば、pinに触れずに `NOT_FOUND` と答える。pinを取った後の確かめ直し（25.3）は、この関数にも当てる。CLIのreader（`RunPin::shared` だけを呼ぶ）は作らない（fileが無ければ「無い」と答える）。

**段4の親のGit領域。** refは `refs/local-ci/snapshots/<source>` と `refs/local-ci/index/<source>` である。消すには、local-ciのライブラリの、sourceを名指してそのrefを消す関数を使う（beltはGit領域の中を自分で書き換えない）。その関数が採用する版に無い場合、この部分は実装できず、refは残る（25.16）。refを消したrepositoryには `git gc --prune=<14日前>` を、repositoryごとに24時間に1回まで行う。`--prune=now` は使わない: 取り込みをするCLIは、refを付ける前のobjectをこのrepositoryに書き、それを知らせるlockを持たないので、猶予なしのpruneは進行中の取り込みのobjectを消す。14日はGitの既定の猶予と同じ値で、利用者のGitの設定に依らないように明示する。したがって、消したsourceのGit objectは、refを消してから最長で14日と24時間残る。

**段6の行を消す順。** 外部キー（`PRAGMA foreign_keys=ON`）を満たすため、子から先に消す。runのattemptとtaskは `task(run_id, task_id)`・`attempt(task_id, attempt_id)` の索引でたどる。

1. `event`: `run_id` がこのrunのもの（新しい索引 `event(run_id, seq)`）と、このrunの各attemptの `attempt_id` を持つもの（既存の索引 `event(attempt_id, seq)`）。後者は、`run_id` がNULLでattemptだけを持つevent（`submit_deferred`・`worker_excluded` など、`Coordinator::note` が書くもの）を拾う。後者で消すのは `run_id` がNULLかこのrunのものだけで、このrunのattemptを名指して別のrunを名指すeventは、そのrunのものとして残し、数える（この版の書き手は、どれもattemptの属するrunを名指すので、作られない）。行の無いattemptを名指すeventは、`append_event` と `append_events_unseen` が `NOT_FOUND` で断る（どのrunの刈り取りにも拾われないeventを作らない）。
2. `result_delete_claim`・`pending_delete`（このrunのlocatorのもの。段3で空になっているはずで、残っていれば段3へ戻る）。
3. `result_gc_ref`、`blob_ref`（attemptごと。`result_gc_ref` は `blob_ref` の削除に連鎖する）。
4. `workflow_download`（consumerがこのrunのattempt）、`workflow_upload_inflight`、`workflow_artifact`（`run_id`）。
5. `artifact_receipt`、`collection_log`、`collection`、`retire_tombstone`、`matrix_permit`（attemptごと、および `matrix_group` 経由）、`placement`（taskごと）。
6. `attempt`。
7. `matrix_group`、`ci_driver_transition`、`run_cancel_intent`、`control_request`（`run_id`）。
8. `task`。
9. `run`。同じtransactionで、どのrunも参照しなくなった `source_ref`・`secret_bundle_ref` の行（新しい索引 `run(source_id)`・`run(secret_bundle_id)` で確かめる）。このtransactionだけは、sourceのlockを排他で、待たずに取ってから行う。submitは、sourceのlockを共有で持ったまま `source_ref` を書き、その後の別のtransactionでrunの行を入れる。排他で持つ間はその2つの間にsubmitが居ないので、変わらないtreeの再submitが、消えた `source_ref` を外部キーで指して失敗することは無い。lockが取れなければ（submitか取り込みの最中）、このrunは次のpassが続ける。

1つのtransactionの中でもこの順を守る。途中のtransactionで止まっても、残っている行は常に「親の行がある子」だけなので、外部キーは満たされる。

**`control_request`。** `run_id` の列と索引は段階1が足している（25.4の (b)）。採らなかった案は「答えを返した時点で消す」である: この行の読み手は、同じ `request_id` の再送（答えを失ったCLIのやり直し）で、再送がもう来ないことを証明できる出来事が無い。runと一緒に消せば足りる: 印のあるrunへの再送は、受領を引く前に印で断られ（25.3）、runの行が無くなった後の再送は、cancelなら「行の無いrunへのcancel」として `NOT_FOUND` になり（25.4の (b)）、`retry` は対象のrunが無いので `NOT_FOUND` になる。どちらも受領の行を必要としない。

**外すtrigger。** `retire_tombstone_no_delete` を外す。`event_no_delete` は、段階1が「行の無いrunのeventの削除だけを通す」条件つきにしてある（25.4の (b)）。刈り取りは、runの行が在るうちに、そのrunのeventと、`run_id` がNULLでattemptだけを持つeventを消す（下の1）ので、このtriggerのままでは断られる。段階2は、このtriggerを外すか、条件を広げる（印のあるrunのeventと、そのattemptのevent）。identityの記録を消す版では `worker_identity_tombstone_no_delete` も外す（下）。消すのは、この章の刈り取りと保持driver、放棄された取り込みの処理だけである。

**workflow artifact。** あるattemptがuploadしたworkflow artifactは、そのattempt自身の保持期間ではなく、**runが終わってから** `retention_days` 日まで残す（後のJobとそのretryがdownloadするため）。今の実装（`claim_result_page` と `workflow_run_end`）が既にそうしている。刈り取りの段3は、runが終わって保持期間が過ぎた後なので、これと矛盾しない。25.10の「結果」の行は、この例外を含めて書く。

**FrozenPlan（`ci-plans/<digest>`）。** 親にもある。内容のdigestで名指され、複数のrunが共有するので、刈り取りは触れない。既存の規則（名指す待ちのtaskが無くなって `retention_days` 日。`src/service/resident/ci_runs.rs`）が消す。

**結果のlocator。** beltのlocatorは `results/<attempt>/…` で、attemptを含む。attemptは1つのrunにしか属さないので、locatorがrunをまたいで共有されることは無く、段3が他のrunを待って止まることは無い（`claim_result_page` の「他のrunが同じlocatorを持つ」検査は、beltが作ったlocatorでは成り立たない）。

**刈り取ったrunを指す参照。** 刈り取りは他のメンバーの記録を変えない（24.1）。刈り取った行を作り直すことは、どの場合もしない。

| 参照 | 答え |
| - | - |
| 利用者の `show`・`logs`・`outputs`・`retry`・`cancel` | `NOT_FOUND`。メッセージで、終了から `retention_days` 日でrunを消すことを伝える（runを名指した場合。taskを名指した場合は、taskの行が無いので、どのrunのものだったかを言えず、従来の `NOT_FOUND` のままである）。cancelも何も書かない（刈り取ったrunには `capture_owner` の行が無いので、25.4の (b) の規則で断られる。`run_cancel_intent`・`event`・`control_request` の行はできない） |
| 同じ `request_id` で送り直したcancel・`retry` | 刈り取りの途中（印がある間）は `run_pruned`、後は `NOT_FOUND`。保存した答えは返さない（25.3） |
| workerに残る、親DBに行の無いattempt | 親は、自分の行があるattemptについてだけworkerへ尋ねる（追跡は行から始まる）ので、行の無いattemptの答えを受け取る経路は無い。workerの答え（`inspect` の件数、eventなど）に行の無いattemptが現れても、親は行を作らず、leaseも更新せず、そのworkerの他のattemptの処理を止めない。workerの側は、段階3の照合（25.6）が消す |
| 遅れて届いた古いsubmitの写し | workerの記録が断る（今は消さないtombstoneとattemptの行、段階3からは重複防止の記録） |
| 古い版のreaderが開いた、消されたpin | inodeの照合で「無い」になる（25.1の (B)） |

**runに属さない記録。**

- **runもattemptも持たないevent**（起動、epoch、GCの失敗の記録など）: 診断のために読む。書いてから `retention_days` 日で、`seq` の小さい順に消す。1 passで1,000行まで。新しい部分索引 `event(seq) WHERE run_id IS NULL AND attempt_id IS NULL` を使う。
- **`ci_test_timing`**: Shardの分割が読む。行に最後に書いた時刻を足し、14日（setup templateの放置の日数と同じ。読み手は「最近動いたtestの時間」だけを使う）書かれなかった行を消す。1 passで1,000行まで。
- **`removed_member` と `removed-members.json`**: 外したメンバーを断るために読む。coordinator DBのあるhostでは表が正で、以前の形式の `removed-members.json` は、読んで表へ移した時点で消す。読めないfileや移せないfileは、警告を出してそのまま残し、`group rotate` は失敗しない（次のrotateがまた試す）。coordinator DBの無いhost（workerだけのhost）では、移す先が無いので `removed-members.json` が正のまま残る（`src/service/group/rotate.rs`）。これは固定のfile（1個。中の項目は利用者が外した名前の数）として25.10に挙げる。どちらも、そのメンバーを戻したときに項目を消す。
- **退役させたidentityの記録**（`worker_identity_tombstone`・`identity_retired_notice`）: 消す時点は未決である（25.16）。段階2では消さず、triggerも外さない。
- **`stats/costs-v1.sqlite`**: 配置の見積が読む実測。`costs` は4,096行、`preparation_namespaces` は4,096、`preparation_receipts` と `cost_eviction` はそれに従う。書くたびに古いものを消す（既存）。

SQLiteのfileは、行を消しても縮まない。空いたpageは次の書き込みが使うので、fileの大きさは、保持期間の中の最大の行数で頭打ちになる。定期の `VACUUM` は行わない（全体を書き直す費用が、縮む量に見合わない）。

**安全の証明。** 段ごとに、まだ読める・書ける相手を挙げる。

- 印の後、新しいattempt・task・retryは作られない（25.3）。したがって、段2以降が消すものを新しく指す行はできない。
- 段3の結果: fileの読み手は共有pinを持つ。排他pinを取れたときだけ消す。印の後に始まる読み手は `NOT_FOUND` で、pinを取りに来ない。
- 段4の入力: 読み手は配送・workspaceの準備・retryで、どれも印か「終わっている」で居ない。削除はBの保護（排他、claimを先に、所有するpathだけ、内容の検証）をそのまま使う。
- 段5のlock: `sources/runs/<run>.lock` は25.1の (A)。
- 段6の行: 読み手は印で断られている。他のメンバーはこのDBを読まない。
- 段8のpin: 25.1の (B)。pinを開けるのは、親DBを開けた版のprocessだけで、その全員がinodeを照合する。readerは作らない。作る側は3つで（上）、`result_gc_guard` は、行があって印が無いときだけ作り、刈り取りと同じdriverの中で重ならずに動く。受理の経路が作るのは、新しいrun IDについてだけである。attemptを名指す関数は、行があって印が無いときだけ作る。この関数は刈り取りと同時に動き得るが、印は段1で書かれて取り消されないので、段8のunlinkより後のsnapshotは、必ず印か行の不在を見る。印より前のsnapshotで作って共有pinを取った場合は、刈り取りが段3で排他pinを取れずに待つ。確認とpinの作成の間に、刈り取りが段1から段8まで進んだ場合だけ、行の無いrunのpinが1個残り得る。これは、pinの後の確かめ直しが `NOT_FOUND` を返した時点で、その関数が、排他で取り直せたときにunlinkする（取れなければ、起動時の走査が消す。25.11）。

**受け入れ試験。** (i) 条件を満たしたrunが、表の8段を経て、25.10の「親のstate directory」のrunごとの行をすべて残さない（fileとDBの行を列挙して確かめる）。(ii) 各段の直後でresidentを止め、再起動後のpassが続きを行い、同じ結果になる（段ごとに1つ）。(iii) 外部キーを有効にしたまま、1 transaction 1,000行で、3,000行を超えるrunを消せる。(iv) `run_id` がNULLでattemptを持つeventが残らない。(v) readerが共有pinを持つ間、段3が進まず、readerが開いたfileは消えない（印の後の次の行の読み取りは `NOT_FOUND` で、追いかけて読むreaderは途中で止まり得る。25.3）。(vi) 刈り取りの途中のどの時点でも、`show`・`ls`・`ci status --post` が `NOT_FOUND` か何も出さない。(vii) 同じsourceを別のrunが参照していれば、manifest・object・bundle・refが残る。(viii) workflow artifactが、producerのattemptの保持期間を過ぎても、runが終わって保持期間が過ぎるまで残る。(ix) workerだけのhostで `removed-members.json` が残り、外したメンバーを断り続ける。(x) retire済みのattemptを持つrunが刈り取られる。(xi) `git gc` が `--prune=now` を使わない。(xii) 刈り取った後、workerがそのrunのattemptをまだ持っている状態で接続しても、親に行ができず、同じworkerの他のattemptの追跡と回収が進む。(xiii) 結果のGCのcursorが指すrunを刈り取り、その後の結果のGCのpageを何回動かしても、`reader-pins/<run>.lock` ができない。印のあるrun・刈り取ったrunのattemptを名指して共有pinを取ろうとしても、pinができず `NOT_FOUND` になる。(xiv) 刈り取ったrunへのcancelと、同じ `request_id` の再送が `NOT_FOUND` で、親DBの行が増えない。(xv) 段階2より前のschemaの版のCLIの `logs`・`outputs` が、DBを開けずに終わり、`reader-pins/` に何も作らない。

**schema。** 親DB: `retire_tombstone_no_delete` を外す。`event(seq) WHERE run_id IS NULL AND attempt_id IS NULL`、`run(source_id)`、`run(secret_bundle_id)` の索引。印のあるrunを探す部分索引 `run(run_id) WHERE pruning_at_ms IS NOT NULL`。親の行を消すときに子を探す索引 `placement(task_id)`・`matrix_group(run_id)`・`matrix_permit(group_id)`（無いと、外部キーの確認が表の全走査になる）。`ci_test_timing` に最後に書いた時刻（`written_at_ms`。既存の行は、この段階のmigrationの時刻から数える）と索引。`event_no_delete` は条件を広げてある: 断るのは、行があって印の無いrunのeventと、`run_id` がNULLでそのようなrunのattemptを名指すeventの削除だけである（上の「外すtrigger」。段階1は、行の無いrunのeventの削除だけを通す条件つきにしていた）。（`event(run_id, seq)`、`control_request.run_id` は段階1。）schemaの版を上げる。`sources/retention.db`: `import_session(run_id)` の索引。wireの変更は無い。

## 25.6 段階3: workerの側の結果の写しと、重複防止の記録

**今の挙動（段階3を入れた後）。** この節の規則のとおりである。機能 `attempt_release_v1` を合意した接続では、workerは `ack_collected` か `release_attempt` を受理した時点で、attemptの行・子・event・tombstoneを消し、重複防止の記録を書き、directoryとlockを消す（`src/storage/worker/release.rs`、`src/worker/agent.rs`）。合意していない接続（段階3より前の親）の `ack_collected` は、今までどおり `acked_at_ms` を書くだけで、その行は次のagentの起動で (c2) の手順で消える。段階3より前は、workerは `ack_collected` を受理した後も7日、ACKが来ないときはterminalから7日（`ack_wait`）で、resultとattemptのdirectoryを消し、`worker_attempt` の行、event、受理したattemptのtombstoneは消さなかった（triggerが禁じていた）。

### (a) ACKと放棄

**`ack_collected` は変えない。** 第13章の4と5のとおり、`generation` と `result_digest` の一致を求め、digestを照合した読み取りの後に保存されたresultを、ACKの記録と同じtransactionで断る。この検出は残す。

**新しい操作 `release_attempt`。** 「このメンバーは、このattemptについてもう何も読まず、submitも送らない」を表す。項目は `attempt_id` だけで、generationもdigestも運ばない（刈り取った後の親は、どちらも持っていない）。機能 `attempt_release_v1` を `hello` で合意した相手にだけ送る。合意していない相手との間では、双方とも今の挙動のままである（workerは `ack_wait` で結果を消し、記録は残る）。段階3より前の親から見える違いは次の3つだけである。(1) ACKしなかったattemptは `ack_wait` の後、行の代わりに解放されていない記録になり、その親は `release_attempt` を送れないので、記録はそのメンバーの上限 10,000 に数え続けられる。上限に達すると submit は `OVERLOADED` で断られ、その親は `overloaded_reason` を知らない理由（`unknown`）として読み、ふつうの `OVERLOADED` として扱う。(2) `belt group forget` で忘れられた親の証明は `member_forgotten` で断られ、段階3より前の版の `group_refusal` には知らない理由の受け皿が無いので、その親はこの答えを読めず、接続の失敗として扱う。段階3からは `group_refusal` も `overloaded_reason`・`conflict_reason` と同じく、知らない理由を `unknown` として読み、ふつうの断られた証明として扱う。(3) それ以外のwireの答えは変わらない。belt は公開した版が無いので、この親は実際には動いていない。

- workerは、terminalのattemptへの `release_attempt` を受理し、(b) の削除を行う。同じ要求の再送には同じ答えを返す。行の無いattemptには `NOT_FOUND` を返す（既に消してある）。
- terminalでないattemptには、後で送り直せる答え（`OVERLOADED`）を返し、何も変えない。そのattemptは、leaseの失効か既存のretireで止まる。
- resultを持たないterminalのattemptでは、`ack_collected` と同じく、supervisorのlockを取れたときだけ受理する（supervisorが生きている間は後からresultが保存され得る）。
- 試用中のworkerは、他の状態を変える要求と同じく、何も行わずに `OVERLOADED` を返す（24.8）。

**親が送る時点。**

| 回収の閉じ方 | 送るもの | 時点 |
| - | - | - |
| 成功（`complete`・`required_missing`） | `ack_collected`（既存） | 検証と永続化の後（第13章） |
| 打ち切り（`gave_up`） | `release_attempt` | 回収を閉じたtransactionのcommitの直後。打ち切った回収のworkerの写しを、もう一度読む処理が無い（下） |
| retireの確認、workerが受理しなかったattempt | 閉じた時点では何も送らない | 親が自分でterminationを確定したもの。受理しなかったattemptは、workerに行が無い。retireを確認したattemptは、刈り取りの段7で送る（確認の時点で送れるかは未決。25.16） |
| 上のうち、ACKも解放も済んでいないattempt | `release_attempt` | runの刈り取りの段7（25.5）と、接続時の照合（下） |
| 親DBに行の無いattempt | `release_attempt` | 接続時の照合（下） |

**決定: 打ち切った回収は、打ち切った時点で解放する。** workerの写しを残すのは、それをもう一度読むものが在る間だけである。打ち切った回収を読み直す処理は、開き直しの保存処理（`reopen_collection`。`src/storage/coordinator/collect.rs`）だけで、それを呼ぶ経路は製品に無い（定義だけがある）。読み手が無いので、打ち切りの時点で `release_attempt` を送り、workerはその場で写しを消す。開き直す経路を後から足す変更は、その経路が、workerに写しを残す上限つきの期間（いつまで、何bytesまで、何が消すか）を自分で定める。それまで、打ち切った回収は開き直せない（開き直しても、workerは `NOT_FOUND` と答え、回収は再び打ち切られる。今も `ack_wait` を過ぎれば同じである）。

`ack_collected` は送らない（採らなかった案「回収を閉じたら、閉じ方に依らずACKを送る」）。打ち切った親はdigestを持たず、今のworkerはdigestの一致を必須にしている。照合を外すと、第13章の4の検出（digestを照合した読み取りの後に保存されたresultを断る）が無くなる。`release_attempt` は別の操作なので、成功した回収のこの検出は変わらない。打ち切った回収では、親は後から保存されたresultを読まないので、検出する相手が無い。resultを持たないterminalのattemptへの `release_attempt` を、supervisorのlockを取れたときだけ受理する規則（上）は残す。

**`ack_wait` の7日。** 段階3の後、7日が効くのは、親がACKも `release_attempt` も送れなかったattemptだけである: 親が7日のあいだ接続しなかったもの、親がまだ回収を閉じていないもの（待ち続けている回収、`observation` がunknownのもの）、retireを確認したもの。成功した回収と打ち切った回収の写しは、閉じた時点で消える。

**接続時の照合。** 親は、workerとの接続を確立するたびに1回、そのworkerに残る自分のattemptのうち、terminalで、ACKも `release_attempt` も受けていないものを、新しい読み取り操作 `list_unreleased` で列挙する。要求は `after`（attempt ID）と `limit`（500まで）を持ち、答えはattempt IDの昇順の1 pageと、続きがあるかである。workerは主キー `(coordinator_id, attempt_id)` で引く。親は、各IDについて自分のDBを主キーで引く。**行が無ければ `release_attempt` を送る。行があり、回収が `gave_up` で閉じていれば、`release_attempt` を送る**（打ち切りの時点で送った解放が届かなかった分を拾う。親は、解放が受理されたことを記録しない。workerが列挙しなくなることが、受理の記録である）。それ以外の行があれば何もしない（通常の回収が扱う）。

行が無いことは、「この親はそのattemptをもう送らない」の証明である: 親は、submitを送る前に必ずattemptの行を作り、行を消すのは刈り取りだけで、attempt IDは使い回さない。回収が閉じていることは、attemptがterminalであることを含むので、同じ証明になる（下の (c)）。試用のrollbackは行を戻すが、試用中のcoordinatorは刈り取りも `release_attempt` も行わない（25.1の8）。

`list_attempts` は使わない（該当が500件を超えると `INVALID_INPUT` を返し、page送りが無いので、溜まった後は列挙できない）。

### (b) workerが消すもの

| 時点 | 消すもの | 証明 |
| - | - | - |
| attemptがterminalになった | workspace（fast-worktreeの削除とportoolの解放。22.5）、置いたsecret fileとその索引、`ci/` のscratch、`tmp/`（既存） | terminalの記録と、supervisorのlockを取れたこと |
| `ack_collected` か `release_attempt` を受理した | `worker_attempt` の行、その子（`command_launch`・`lease`・`log_spool`・`blob_ref`）、そのattemptのevent、受理したattemptのtombstone。同じtransactionで、**解放済み**の重複防止の記録を1行書き（既に未解放の記録になっていれば、解放済みに変える）、attempt IDを `dropped_attempt` に書く。commitの直後に、`attempts/<attempt>/`（result・log・artifact・`supervise.json`）を消し、次に `supervisors/<attempt>.lock` を消し（下）、最後に `dropped_attempt` の行を消す。待たずにその場で行う | 親がもう読まないと述べたこと。回収の後にworkerの写しを読む処理は無い |
| ACKも `release_attempt` も来ないままterminalから7日（`ack_wait`） | 上と同じもの。同じtransactionで、**未解放**の重複防止の記録を1行書く。対象は行で選ぶ（directoryが既に無い、段階3より前の行も含む） | 回収に来る親が7日居なかったこと（既存）。行の残りの読み手は遅れたsubmitの照合と `get_attempt` だけで、記録が答える |

ACKの後に7日残していた規則はやめる。回収が済んだ結果を読むのは親の写しだけである。

**消す順序と、途中で落ちたとき。** 順序は「行（と記録、`dropped_attempt`）のcommit → `attempts/<attempt>/` → `supervisors/<attempt>.lock` → `dropped_attempt` の行」である。`dropped_attempt` は既存の表で、起動時の既存の片付け（`remove_dropped_attempts`）が、残っている行のattemptについて同じ順序の続きを行う。したがって、どこで落ちても、directoryとlockは次の起動で消える。

**`supervisors/<attempt>.lock` を消す条件（25.1の (A)）。** このpathを開く処理は、次のものがすべてである。

| 開く処理 | 場所 | 開く条件 | unlinkの後に開かない理由 |
| - | - | - | - |
| supervisorの起動（fileを作る） | `src/supervisor/process.rs` | agentがそのattemptのsupervisorを起動したとき | attemptの行が無い。attempt IDは使い回さない。agentがlockを取れたので、起動済みのsupervisorも居ない |
| agentの `drive` | `src/worker/agent.rs` | terminalでないattemptを進めるとき | 行が無い |
| agentの `settling`（`workers stop` の集計） | 同上 | terminalでないattemptを数えるとき | 同上。今は `supervisors/` の全部を見ているので、worker DBのterminalでないattemptだけを見るように改める |
| agentの `ack`（resultの無いattemptのACK・解放） | 同上 | 行のあるattemptの要求を処理するとき | 行が無ければ、lockを開く前に記録が答える |
| agentの `interrupt_offline`（`group send-key` が、agentを止めた状態で古いattemptを止めるとき） | 同上 | terminalでないattemptについて | 行が無い |
| agentの `settle`（復旧） | 同上 | 起動時に、terminalでないattemptについて | 行が無い |
| 別のscopeで動くsupervisorの照合 `config::traffic::foreign_supervisors` | `src/config/traffic.rs` | `attempts/*/supervise.json` と `setup/builders/*/spec.json` を読み、scopeが違うものについてlockを取る。台帳は読まない | `attempts/<attempt>/` を、unlinkより先に消してある。`supervise.json` が無ければ、そのattemptのlockを開かない |

unlinkを行うのは、`dropped_attempt` の続きを行う処理（`remove_dropped_attempts`。今はlockに触れない）である。段階3で、directoryを消した後に、lockを**作らずに**取り、取ったままunlinkしてから手放す処理を足す。fileが無ければ何もしない。取れなければ（supervisorが生きている。terminalのattemptでは起きないはずである）、`dropped_attempt` の行を残して次の起動に回す。`remove_dropped_attempts` は、`worker_attempt` に行が残るattemptを飛ばす（`attempt_id_held`）ので、行を消すtransactionのcommitの後にだけ働く。

最後の行の照合を呼ぶのは、`config::traffic::hold` で、呼び出し元は3つある: workerの起動（`src/service/worker.rs`。`agent.lock` を取った後）、residentの起動（`src/service/resident.rs`。**`agent.lock` を取る前**。`agent.lock` は、その後のagentの開始の中で取る）、`ResolvedConfig::worker_runtime`（`src/config/runtime.rs`。両方の起動から呼ばれる）。したがって、この照合は `agent.lock` で守られているとは言えず、動いているagentの削除と同時に走り得る。次の2つを規則とする。

- **directoryが先、lockが後。** 上の順序のとおり、`attempts/<attempt>/` を消してからlockをunlinkする。照合が `supervise.json` を読めるのは、unlinkより前だけである。
- **照合は、fileを作らずに取る。** `SupervisorLock::try_acquire` は、確かめるだけの呼び出しでもfileを作る。段階3で、作る取得（supervisorの起動だけ）と、作らない取得（agentの5か所、unlinkの処理、`foreign_supervisors`）に分ける。作らない取得は、fileが無ければ「supervisorは居ない」と答える。これで、`supervise.json` を読んだ直後にdirectoryとlockが消えても、照合はlockを作り直さない。setup builderのlock（`src/setup/builder.rs`。builderのdirectoryの中）は、builderと一緒に消えるので変えない。

段階3より前の版の `hold` は、作る取得のままである。その版のresidentかworkerが、段階3のagentの削除と同時に起動の途中にあり、`supervise.json` を読んだ後・directoryが消えた後にlockを取ると、空のlock fileが1個できる。害は無い（そのattemptのsupervisorは二度と起動されず、fileの有無を「supervisorが居る」と読む処理は無い）。残ったfileは、次のagentの起動時の走査（25.11）が、行が無いことを見て消す。

**全走査をやめる。** 「まだ動いているsupervisorの数」は、`supervisors/` を読まず、worker DBのterminalでないattemptのlockだけを直接のpathで調べる。

### (c) 重複防止の記録

第11章の「古いsubmitを新規実行する状態に戻さない」ために、消したattemptごとに小さい記録だけを残す: `coordinator_id`、`attempt_id`、`payload_hash`、`coordinator_epoch`、終わり方の種類、解放済みかどうか、時刻（1件約200 bytes）。cancel・retireのtombstoneのうち、attemptを受理していたもの（`delivered = 1`）は、この記録に置き換える。submitの「新規かどうか」の判定（`src/worker/agent.rs` の `fresh`）は、attemptの行、tombstone、この記録のどれも無いことにする。**この判定と、attemptの行の挿入は、worker DBの1つの書き込みtransactionで行う。** 今の実装は、判定をprocessの中のlock（`submitting`）の下で行い、挿入はその後の別のtransaction（`submit_within`）で行う。解放の受理（行を消して記録を書く）と、記録の削除は、それぞれ1つの書き込みtransactionなので、判定と挿入を同じtransactionに入れれば、SQLiteの書き込みの直列化だけで「判定の後、挿入の前に記録が消える」ことが無くなる。

- **読み手**: 遅れて届いたsubmitの写し（同じ `attempt_id` を、`RETIRED` または既存の終わり方で答える）と、親の `get_attempt`（終わり方の語だけを答える。resultは「期限切れ」）。
- **解放済みの記録を消す時点**: その解放（ACKか `release_attempt`）より前に、そのメンバーの接続から読まれ得たsubmitが、すべて処理を終えたとき（下の「解放済みの記録を消してよい条件」）。通常は、解放の受理から数ミリ秒の後である。接続が閉じるのを待たない。
- **未解放の記録**（`ack_wait` を過ぎたattemptのもの）: 親が `release_attempt` を送るまで消さない。親はそのattemptを未確定として持っている可能性があり、送り直し得る。`release_attempt` を受けたら解放済みに変え、上の規則で消える。`list_unreleased` は、terminalで未解放のattemptの行と、未解放の記録の両方を列挙する。
- **上限**: メンバーごとに10,000件（解放済みと未解放の合計。約2 MB。届かなかったtombstoneの上限と同じ値）。上限に達したメンバーからの新しいsubmitは、`OVERLOADED`（`overloaded_reason.reason = "attempt_records"`）で断る。他のメンバーのsubmitには影響しない。この答えを受けた親は、そのworkerについて照合（`list_unreleased`）を行う。
- **上限が受け持つもの。** 解放済みの記録は、下の条件で、解放の直後に消える。同時に在る解放済みの記録は、「処理の途中のsubmitが在る間に受理した解放の数」で、submitの処理は1件が1つのtransactionと検査だけであり、同時に動く要求はagent全体で64（`MAX_CONCURRENT_REQUESTS`）なので、通常は0か数件である。1本の接続を長く保つ親でも、記録は溜まらない。したがって、上限に届くのは未解放の記録だけで、それは「このメンバーが、7日のあいだACKも解放も送らず、今も行を持っているattempt」（回収を閉じていないもの、unknownのもの、retireを確認したもの）が10,000件を超えた状態である。この数を実装から導く上限は無い（worker 1台が7日に終えるattemptの数は、利用者の使い方で決まる）ので、歯止めとして10,000件を置く。値は、届かなかったtombstoneの上限に合わせた。断りは日常の経路ではなく、回収しない親の異常の表示である。
- **戻らないメンバー**: `belt group forget`（下）を実行しないまま居なくなったメンバーの記録は、上限（10,000件、約2 MB）までで止まり、それ以上は増えない（新しいsubmitが来ない）。消すのは `belt group forget` である。メンバーの数は利用者の操作で決まる。

**解放済みの記録を消してよい条件。** workerは、接続から要求を順に取り出し、取り出した要求を、別々の処理として同時に動かす（`src/worker/serve.rs`。取り出しは接続ごとに順番、処理はagent全体で64まで同時で、終わる順は取り出した順と限らない。接続が終わっても、取り出し済みの要求の処理は最後まで動く）。したがって、「接続が閉じた」は「その接続から読んだsubmitの処理が終わった」を意味しない。条件は、接続ではなく、要求の処理で書く。

workerは、接続ごとに、取り出したsubmitへ通し番号を付け、処理の途中のsubmitの番号の集合をメモリに持つ。接続が**済んだ**とは、その接続からもう何も取り出さず（接続が終わり、取り出しの口を捨てた）、取り出したsubmitの処理がすべて終わったことである。

接続Cで受けた解放（Cがそれまでに取り出したsubmitの数をrとする）の記録は、次の2つが成り立った時点で消す。

1. Cで、解放より前に取り出したsubmit（番号がr以下）の処理が、すべて終わった。
2. 解放を受けた時点で開いていた、同じメンバーの他の接続が、すべて済んだ。

確かめるのは、submitの処理が1つ終わったときと、接続が1つ済んだときである（その出来事が条件を満たした記録だけを、主キーで消す）。この表は永続しない。agentが再起動した時点で、接続も処理の途中の要求も無いので、起動時に、接続を受け付ける前に、解放済みの記録を全部消す（1 transaction 1,000行まで）。

**これが証明になる理由。** submitの写しが存在し得る場所は、親のprocessと、接続の中と、workerが取り出して処理している途中だけである。

- 親は、submitを、その時点でそのworkerに付いている接続にだけ書く（`Coordinator` は、送る直前に、尋ねた接続が今も付いているものと同じかを確かめる）。接続が切れた呼び出しは答えなしで終わり、別の接続へ自動では送り直されない。
- 親は、attemptのterminalを記録した後、そのattemptのsubmitを送らない。ACKと `release_attempt` は、terminalの記録か行の不在の後にだけ送る。**規則として**: 親は、あるattemptのsubmitの呼び出しが答えを待っている間、そのattemptのACK・`release_attempt` を送らない。
- したがって、親が解放を書いた時点で、そのattemptのsubmitの呼び出しは、どれも終わっている。終わり方は2つである。答えを受け取った（workerは処理を終えている）。接続が切れて答えなしで終わった（写しは、親が既に見限った接続の中に残り得る）。
- **解放と同じ接続Cの中の写し。** submit・`ack_collected`・`release_attempt` は、どれも同じ種類（control）の要求で、1本の接続の中では、親が書いた順にworkerが取り出す（`src/protocol/` の送る側と受ける側のqueueは、種類ごとに先入れ先出しである。**規則として**、`release_attempt` はcontrolの種類とし、この順序を保つ）。親は解放の後に同じattemptのsubmitを書かないので、Cの中の写しは、解放より前に取り出されている（番号がr以下）。条件1は、その処理が終わるのを待つ。処理は、記録を見て、実行せずに答える。
- **他の接続の中の写し。** 親が見限った接続である。親は自分の側を閉じているので、workerは残りを読み終えると接続の終わりを見る。条件2は、その接続から取り出したsubmitの処理が終わるのを待つ。同じメンバーの名義で接続する他のprocess（`doctor`・`workers`・`group` の確認の接続）はsubmitを書かないが、workerはそれを知らないので、同じく数える（記録を消すのが遅れるだけで、早くはならない）。これらは1回の問い合わせで閉じる接続である。
- 新しい接続は、親の新しい `hello` から始まる。SSHの中継は、切れた接続のbytesを別の接続へ送り直さない（今の中継に、つなぎ直して送り直す処理は無い。**規則として**、足さない）。解放より後に開いた接続に、解放より前のsubmitの写しは入らない。
- submitを書くのは、residentの `Coordinator` だけである。親が再起動するとepochが進み、古いepochの写しは `STALE_EPOCH` で断られる。
- 記録が在る間に処理されたsubmitは、判定と挿入が1つのtransactionなので、必ず記録を見る。記録を消すtransactionは、上の2つの条件が成り立った後にだけ始まる。その後に同じattemptのsubmitを取り出す接続は無い。

**残る遅れ。** 条件2は、解放の時点で、同じメンバーの長く開いたままの接続がもう1本ある場合に、その接続が終わるまで待つ。residentがworker 1台へ付ける接続は1本で（`src/config/runtime.rs` の `attach_worker_link`）、他は1回の問い合わせで閉じるので、通常は起きない（residentが同じworkerへ2本目の長い接続を開く経路が無いことは、段階3の実装で確かめ、受け入れ試験の (ix) が守る）。起きた場合、記録は上限までで止まり、その接続が終わった時点でまとめて消える。

時間だけで消す規則は採らない（接続の中に残った写しが届かないことを、時間では証明できない）。メンバーのepochの前進も、解放済みの記録を消す条件には使わない（上の規則で足りる）。届かなかったattemptのtombstone（24.4）は、従来どおりepochの前進で消す。

### (c2) 段階3より前に作られた行

**今の挙動。** 段階3より前のworkerは、`ack_collected` を受理しても `worker_attempt` の行を消さない（`acked_at_ms` を書くだけ）。directoryは7日後に消える。段階3の規則は「ACKの受理の時点で消す」なので、既に受理した行にはその出来事が二度と来ない。`list_unreleased` はACK済みの行を列挙しないので、照合も拾わない。何もしなければ、段階3より前にACKされた行は永久に残る。

**規則。** 段階3のagentは、起動時に、接続を受け付ける前に、`acked_at_ms` を持つterminalの行を、(b) の「ACKを受理した」と同じ手順で消す（行と子とevent、受理したattemptのtombstone、`dropped_attempt` 経由のdirectoryとlock）。1 transaction 1,000行までで、起動時に終わらなかった残りは、tickが同じ手順で続ける。**重複防止の記録は書かない。**

- **記録が要らない理由。** その行は、親が送った `ack_collected` をworkerが受理したものである。親は、ACKを送る前にattemptのterminalを記録しており、terminalの後はsubmitを送らない。起動時には接続が無く、処理の途中の要求も無いので、「解放済みの記録を消してよい条件」は最初から成り立っている（書いても、その場で消える記録である）。tickが続きを行う時点では接続が在るが、その接続は起動の後に開いたもので、ACKより前のsubmitの写しを含まない。
- **ACKの答えを失った親。** 親がACKの受理を記録していない場合、親はACKを送り直し、行を消したworkerは `NOT_FOUND` と答え、親は回収を閉じる（第13章の5。既存の規則）。
- **試用中は行わない。** この削除は、試用中（24.8）は行わない（25.1の8。rollbackはworker DBを戻すが、消したdirectoryは戻せない）。schemaの変更（表を足し、triggerを外す）だけを試用の中で行い、削除はcommitの後の最初の起動かtickから始める。
- **ACKされていない、段階3より前の行。** terminalから7日を過ぎた行は、directoryが既に無くても行が残っている。(b) の `ack_wait` の規則は行で選ぶので、最初のtickが、行を消して未解放の記録に変える。7日以内の行は、通常の規則（ACK、`release_attempt`、`ack_wait`）で消える。
- **受理したattemptのtombstone（`delivered = 1`）。** 段階3より前は、行と一緒に残っている。行を消す処理が、同じtransactionで消す（上のとおり、記録に置き換える必要が無い）。行の無い、受理したattemptのtombstoneは、段階3より前の版では作られない。

### (d) メンバーを忘れる

**今の挙動。** `workers forget <worker>` は、親が、消えたworkerのidentityを退役させる命令である（第15章）。workerの側で、投げたメンバーの記録を消す命令は無い。

**規則。** 命令を分ける。`workers forget <worker>` の意味は変えない。workerのマシンで、戻らないメンバーの記録を消すのは、新しい命令 `belt group forget <member>` とする。

- 前提: そのメンバーが、このマシンのグループの一覧に無い（外してある）こと。そのメンバーのterminalでないattemptが無いこと（あれば件数を示して断る。leaseの失効で止まるのを待つ）。
- 行うこと（worker DBの1 transaction）: そのメンバーの `worker_attempt` とその子、event、重複防止の記録、tombstone、`member_epoch`、`retired_identity`、`group_member` の行を消し、**`refused_member`（`coordinator_id`、名前、時刻）に1行を書く**。attemptのdirectoryは、その後に消す。
- `refused_member` にある `coordinator_id` の接続は、proofの後に、理由 `member_forgotten` で断る。`group send-key` でそのメンバーに鍵を渡しても、このマシンは受け付けない。そのメンバーが戻るには、stateを作り直して新しい `coordinator_id` を得る。
- `refused_member` の行は消さない。行の数は、利用者がこの命令を実行した回数で、実行の数では増えない。1,024行を上限とし、超える命令は断る。

**二重実行が起きない理由。** 戻ったメンバーの親は、ACKの無い配置のsubmitを送り直す（第11章）。記録を消したworkerは、それを新しいsubmitと区別できない。`refused_member` があれば、その親は接続できないので、submitは届かない。

**決定: 命令を分け、消去を永久の断りに結び付ける。** 採らなかった案は、(1) `workers forget <member>` に2つ目の意味を持たせる（1つの命令が、親のマシンでは「workerのidentityを退役させる」、workerのマシンでは「メンバーを忘れる」になる）、(2) 断りの行を、鍵の更新で消す（`group send-key` は、更新の後の鍵を同じ `coordinator_id` のメンバーへ渡せるので、鍵の更新は「戻らない」の証明にならない）。

### (e) workerのevent

attemptに属さないworkerのevent（起動、identity、GCの記録）は、`retention_days` 日で、`seq` の小さい順に、1 tickで1,000行まで消す。読み手は親の `watch_events` と診断である。attemptのeventは、(b) でattemptと一緒に消す。

**安全の証明（段階3の全体）。** 結果の写しを読むのは、submitしたメンバーの回収だけで、そのメンバーが「もう読まない」と述べた後に消す（成功した回収はACK、打ち切った回収は閉じた時点の解放、刈り取ったrunは行の不在）。attemptの行を読むのは、そのメンバーの `get_attempt`・`get_result`・`watch_events` と、遅れたsubmitの照合で、前の3つは解放の後には来ず、最後のものは重複防止の記録が答える。記録を消した後に同じattemptのsubmitが届く経路は、(c) と (d) で閉じている。

**受け入れ試験。** (i) `ack_collected` の受理の直後に、attemptのdirectory・行・子の行・eventが無く、記録が1行書かれている（処理の途中のsubmitが無ければ、その記録は同じ受理の中で消える）。(ii) `ack_collected` のdigestの不一致と、読み取りの後のresultの保存は、今までどおり断られる。(iii) 回収を打ち切った直後に `release_attempt` が届き、workerのdirectory・行が消える。届かなかった場合（workerが不在）、次の接続の照合が、行があって `gave_up` で閉じた回収として拾い、消える。(iv) 刈り取りのときにworkerが不在でも、次の接続の照合で消える。親DBに行があり、回収が閉じていないattemptには `release_attempt` を送らない。(v) 600件を超える未解放のattemptを、`list_unreleased` がpageで列挙できる。(vi) 解放より前に取り出されて処理を待っているsubmitの写し（処理を止める点を差し込む）は、解放の受理の後に処理されても実行されず、その処理が終わった時点で記録が消える。接続は開いたままである。(vii) 別の接続に残した写し（解放の時点で開いていた接続）が、解放の後に読まれても実行されず、その接続が済むまで記録が残る。接続が終わっても、取り出し済みのsubmitの処理が終わるまで記録が残る。(viii) agentの再起動で、解放済みの記録が消え、未解放の記録は残る。`ack_wait` を過ぎたattemptは、行が無く未解放の記録があり、遅れたsubmitが実行されず、`release_attempt` で消える。(ix) 1本の接続を保ったまま、10,000件を超えるattemptを順に実行・回収しても、解放済みの記録が溜まらず、`attempt_records` の断りが出ない。未解放の記録が10,000件に達したときだけ断りが出て、他のメンバーは断られない。(x) `belt group forget` の後、同じ `coordinator_id` の接続が `member_forgotten` で断られ、送り直されたsubmitが実行されない。terminalでないattemptがあれば命令が断られる。(xi) `workers forget <worker>` の挙動が変わらない。(xii) 機能を合意していない相手には `release_attempt` も `list_unreleased` も送らず、今の挙動になる。(xiii) lockを確かめるだけの呼び出し（agentの5か所と `foreign_supervisors`）が、消したlock fileを作り直さない。`attempts/<attempt>/` がlockより先に消える。directoryを消した直後・lockを消した直後に落ちても、次の起動が `dropped_attempt` から続きを行う。residentの起動の `hold` を、agentの削除と競わせても、lock fileが残らない。(xiv) submitの判定と挿入の間に解放と記録の削除を差し込めない（判定と挿入が1つのtransactionである）。(xv) 段階3より前にACKされた行を持つworker DBで起動すると、接続を受け付ける前に、その行・directory・lockが消え、記録は増えない。1,000行を超える分はtickが続ける。試用中は消えない。段階3より前の、7日を過ぎたACKの無い行は、最初のtickで未解放の記録に変わる。(xvi) 親が、同じattemptのsubmitの答えを待っている間、ACKも `release_attempt` も送らない。

**schema・wire。** worker DB: 重複防止の記録の表、`refused_member`、`event_no_delete`（worker DB）と `tombstone_no_delete` を外す。submitの判定と挿入を1つのtransactionにする。段階3より前の行の変換（(c2)）。wire: 機能 `attempt_release_v1`、操作 `release_attempt`・`list_unreleased`、`overloaded_reason.reason = "attempt_records"`、断る理由 `member_forgotten`。CLI: `belt group forget <member>`。親: submitとACK・`release_attempt` の間の排他、接続時の照合。

## 25.7 段階4: 古いbinaryと、toolchainのroot

### (a) 古いbinary

**今の挙動。** `<config>/bin/<digest>/` と `tool-bin/<digest>/` は、版ごとに1つ増え、消さない。

**規則。** `<config>/bin/<digest>/` を消してよいのは、その版を名指すものが1つも無いと確かめたときである。名指し得るものをすべて挙げる。

| 名指すもの | 確かめ方 |
| - | - |
| link `bin/current`、`bin/resident`（2つは別の版を指すことがある） | 両方を読む |
| 登録済みのserviceの定義（residentとworkerのLaunchAgentのplist、systemdのunit） | 定義のfileを読み、実行するpathを見る |
| system userのworkerが固定した版のpath（`src/service/worker_service/system_user.rs`。`bin/current` を経ずに版のpathを直接実行する） | そのworkerの入力と、動いているprocessの実行file |
| journal（`init.json`、`worker-service.json`、`upgrade/record.json`、`upgrade-trial*.json`） | 各journalの版の項目。試用の戻し先を含む |
| 更新の番人（upgrade guard。前の版のbinaryで動く、KeepAlive付きのservice） | service managerに番人のlabelが登録されていないことと、番人の定義のfileが無いことの両方。`commit` は番人の削除に失敗しても確定するので、record だけでは判定しない |
| setup builder（supervisorと同じく、更新をまたいで動き続ける。worker DBのattemptの数にも `supervisors/` にも出ない） | `setup/builders/` にdirectoryが無いか、あるものすべてのlockを取れること |
| supervisor | worker DBにterminalでないattemptが無いこと（`upgrade` は保留を置いて完了を待つ。24.8） |
| `tool-bin/<digest>/` のlink（hookとcommandがPATHから呼ぶ） | 上のsupervisorとbuilderが居なければ、呼ぶものは居ない |

1つでも名指すものがあるか、確かめられなければ、その版は消さない。試用中は何も消さない（25.1の8）。

- **消す時点**: 更新のcommitの直後（番人の削除の後）。相手のマシンでは、そのマシンのcommitが消す。版の検査で断った写しは、その場で消す（第15章）。
- **受け皿**: residentとworkerの起動時に、同じ確かめを行って消す（commitの時点でbuilderが動いていた場合など）。
- `tool-bin/<digest>/` は、指す先の `bin/<digest>/` と一緒に消す。
- `bin/.resident.<pid>`・`bin/.current.<pid>` は、段階1の名前の規則に移し、切替の完了かrollbackで消す。

動いているprocessの実行fileをunlinkしても、そのprocessは動き続ける。害になるのは、pathで実行し直す経路（supervisorの起動、`tool-bin` のlink）だけで、表はその経路をすべて挙げている。

**受け入れ試験。** (i) commitの後、前の版のdirectoryと `tool-bin` が消える。(ii) 表の各行について、それだけが前の版を名指す状態を作ると、消えない（行ごとに1つ）。番人は、登録だけが残る場合と、定義のfileだけが残る場合の両方。(iii) builderが動いている間のcommitでは消えず、builderの終了後の起動で消える。(iv) 試用中は消えない。

### (b) toolchainのroot

**今の挙動。** `toolchains/<identity>/` は、導入の計画ごとに1つ増え、receiptのあるrootを消す処理は無い。既存のrootを使うattemptは、lockを取らずに `receipt.json` を読む（`toolchain::adapters::detect`）。

**規則。** 参照の記録と削除のclaimを、worker DBの1つのtransactionで直列にする。

- worker DBに、rootの表（identity、状態 `live`・`evicting`、最後に使った時刻、bytes）と、参照の表（identity、参照する者）を持つ。参照する者は、attempt（`coordinator_id` と `attempt_id`）、setup builder（builderのID）、ウォームアップ（`env_sync` の要求とagentの起動の番号）、setup template（templateのkey）である。
- **bind。** 既存のrootを使うattemptは、`receipt.json` を読む前に、1つのtransaction（`BEGIN IMMEDIATE`）で、rootの状態が `live` であることを確かめ、参照を足し、最後に使った時刻を更新する。状態が `evicting` か、行が無ければ、rootは無いものとして扱い、導入の経路へ進む。
- **導入。** builderは、`install.lock` を取り、同じ形で参照を足してから導入し、完了で行を `live` にする。状態が `evicting` のrootへは導入せず、削除の完了を待ってやり直す。
- **参照を外す時点。** attemptはterminalを記録するtransactionで。builderは終了時に。ウォームアップは完了時に。templateは、templateが消えたときに（templateは自分のpinと放置の規則で消える。第8章）。受け皿: builderの参照は、builderのlockを取れたら外す。ウォームアップの参照は、agentの起動の番号が今と違えば外す。
- **削除。** 1つのtransactionで、参照が無いことと下の条件を確かめ、状態を `evicting` にする（これがclaimである）。その後、`install.lock` を取り、22.2の手順（`toolchains` の中のtrashへrenameしてから消す）で消し、最後に行を消す。`evicting` の行は、起動時に続きを行う。
- **対象。** 参照が無く、14日使われなかったroot。合計が20 GiBを超えたら、参照の無いものを、最後に使った時刻の古い順に消す（数値はsetup templateと同じ。読み手はどちらも「次の実行を速くするcache」）。
- **template。** rootの上で作ったtemplateは、そのrootを参照する。rootを消すためにtemplateを消すことはしない。templateが自分の規則（20 GiB、14日、run scopeは1時間、copy中とpinの保護）で消えた後に、rootが対象になる。
- **古い版のbuilder。** 参照を記録しない版のbuilderが動いている間は、どのrootも消さない: `setup/builders/` に、参照の表に無いbuilderのdirectoryがあり、そのlockを取れないか、そのbuilderが起動したhelperが残っていれば（下）、削除のpassを丸ごと見送る。
- **段階4より前に導入されたroot。** 段階4より前のrootには、表の行が無い。行が無いままでは、削除の対象にならず（永久に残る）、bindは「行が無いのでrootは無い」として、receiptのある既存のdirectoryへ導入をやり直す。したがって、段階4のmigrationの後の最初の起動で、agentが `agent.lock` の下で、`toolchains/` を1回だけ読み、`receipt.json` のあるdirectoryごとに `live` の行を作る（最後に使った時刻は、その起動の時刻。bytesは、削除のpassが初めて必要としたときに測る）。`receipt.json` の無いdirectory（導入の途中で止まったもの）には行を作らない。それは、次にそのrootへ導入するbuilderが、`install.lock` の下で片付ける（既存）。行を作り終えるまで、bindと削除を始めない。

**`install.lock`（25.1の (A)）。** 開く経路は3つである。(1) そのrootへ導入するsetup builder（導入の入口 `adapters::install_admitted` を呼ぶ `src/setup/builder.rs` と `src/setup/builder/probe.rs`）。(2) builderが起動する導入のhelper process（`src/toolchain/preparation.rs` の `helper_main`。builderがnodeとして起動し（`src/setup/builder.rs`）、helperが `install_admitted` を呼ぶ。CLIは呼ばない）。(3) workerの起動時のtrashの片付け（`adapters::sweep_trash`。agentが `agent.lock` の下で行う）。

helperはbuilderとは別のprocessである。builderがSIGKILLで終わると、builderのlockは空くが、builderが起動したprocessは残り得る。したがって、「builderのlockを取れた」は、helperが居ないことの証明にならない。**builderの参照を外してよいのは、builderのlockを取れ、かつ、builderのprocess groupと、builderが起動したprocess（helperを含む）のprocess groupに、processが残っていないときである。** groupは、builderのdirectoryの起動の記録（pid・process group・開始identity）から読む。残っていれば、参照は残し、次のtickで確かめ直す。この確認は新しい要求である（helperがどのprocess groupで起動されるか、今のbuilderの片付けがその終了まで確かめているかは、コードから確認できていない。段階4の実装が最初に確かめ、無ければ足す。25.16）。

削除の時点で、参照の表にbuilderが無く（したがって、そのhelperのgroupも無い）、古い版のbuilderとそのhelperも居ない（上）。削除を行うのは今のagentで、(3) と同時には動かない。したがって、開くprocessは残っていない。lockはrootと一緒に消える。

**安全の証明。** rootを読む・書くのは、bindしたattempt、builder、ウォームアップ、templateの適用である。どれも参照を持つ。参照を足すtransactionと、claimを書くtransactionは、同じDBの書き込みとして直列になるので、「参照なしを確かめてからrenameするまでの間にbindされる」ことは無い: bindが先なら参照があり、claimが先ならbindは `evicting` を見る。

**受け入れ試験。** (i) 既存のrootをbindするattemptと削除を競わせ、「bindが成功してrootが残る」か「bindが導入へ進む」のどちらかになる（PATHの先が消えた状態で実行されない）。(ii) terminalでないattempt、動いているbuilder、進行中のウォームアップ、残っているtemplateが参照するrootは消えない。(iii) `evicting` の途中で落ちても、起動時に続きが行われる。(iv) 古い版のbuilderのdirectoryとlockがある間、削除が見送られる。(v) 14日と20 GiBの両方の条件。(vi) builderをSIGKILLし、helperが生きている間、builderの参照が残り、rootが消えない。builderとhelperのprocess groupが無くなった後のtickで参照が外れる。(vii) 段階4より前に導入したroot（表に行が無く、`receipt.json` がある）が、最初の起動で `live` の行を得て、bindが導入をやり直さず、14日使われなければ消える。`receipt.json` の無いdirectoryには行ができない。

**schema・wire。** worker DB: rootの表と参照の表。最初の起動での、既存のrootの行の作成。wireの変更は無い。

## 25.8 段階5: workerが受け取ったsourceとclone

段階5は2つに分ける。**5a（取り込みの持ち主）**と、**5b（受け取ったsourceとcloneの中身の削除）**である。5bは5aの後に入れる。25.9は、5bで閉じなかったことだけを置く。

### 5a: 取り込みのpending rootの持ち主

**今の挙動。** `SourceStore::ingest_blob` は、blobを1つ受け取るたびに `begin_import` を呼び、`sources/retention.db` に `import_session` の行（`pending`）と `root(kind='pending')` の行を作る。workerの受け取り（`src/worker/blobs.rs`）はこの経路だけを使い、公開はtokenを持たない `publish_received_one` で行うので、`finish_import` も `release_import_page` も呼ばれない。pending rootは解放されず、`import_session`・`root`・`member` の行は、受け取ったblobの数だけ増え続ける。親では、`GuardedObjectStore::put_bytes`（`src/source/store.rs`）が同じ形で、呼び出しごとに `import_session` とpending rootを作るが、製品の経路にこれを呼ぶものは無い（呼ぶのはtestだけである）。公開済みのsourceのobjectは、別に `receive:<source>` のroot（種類 `received`）が保護している。`import_session` の行を消す処理は無い（親の、runに結び付いた行も残る）。

**規則。** 次の4つを不変条件とする。

1. **pending rootには持ち主が居る。** workerの受け取りでは、持ち主は受け取りの作業directory（`incoming/<source>/`）である。公開の呼び出し1回につきtokenを1つだけ取り（`begin_import`）、そのIDを、最初のblobを入れる前に作業directoryの中に記録する。blobは、そのtokenで入れる（`ingest_blob_with_token`。既にある）。blobごとに `begin_import` を呼ぶ `ingest_blob` は、workerでは使わない。
2. **公開済みのsourceを保護するrootは、sourceごとに1つである。** manifestの公開は、今と同じ経路（`publish_received_one`）で行う。この経路は `receive:<source>` のroot（種類 `received`）にmanifestのobjectを登録し、同じsourceを何度受け取ってもrootは増えない（登録済みなら何もしない）。tokenつきの公開（`publish_received_with_token`）は、tokenごとのroot（`receive:<token>`）を作り、受け取るたびに1つ増えるので、workerでは使わない。
3. **持ち主が終わるとき、pending rootも終わる。** 公開がすべて済んだ後（すべてのmanifestのobjectが `receive:<source>` のrootに入った後）、tokenを**解放**する: `import_session` の行、tokenのroot（`import:<token>`）、そのmemberを消す（新しい処理）。その後で作業directoryを消す。作業directoryを捨てるとき（拒否、24時間進まなかったとき）も、記録したtokenを同じ形で解放してから捨てる。
4. **持ち主の居ない行は、起動時に消す。** workerの起動時に、`agent.lock` の下で、`incoming/` のどの作業directoryにも記録されていない `import_session` の行と、そのroot・memberを消す。1 transaction 1,000行まで。今までに溜まった、blobごとの行もこれで消える。

親の側: runに結び付いた `import_session` の行（`bound`）は、runの刈り取りの段5で消す（25.5）。放棄された取り込みの行は、25.4の (b) がpending rootと一緒に消す。`GuardedObjectStore::put_bytes` は、製品の呼び出し元が無いので、今は行を作らない。段階5bで消す（下の5bの (f)）。

**安全の証明。** pending rootの役目は、公開の前のobjectを削除から守ることである。workerでsourceのstoreへ書くのはagentだけで（`agent.lock`）、進行中の公開のtokenは作業directoryに記録されている。記録の無いpending rootは、進行中の公開のものではない。公開済みのsourceのobjectは `receive:<source>` のrootが保護しているので、tokenのrootを消しても、公開済みのsourceは欠けない。3の順序（`receive:<source>` への登録が先、tokenの解放が後）により、公開の途中のどの時点でも、objectはどちらかのrootに入っている。どのrootにも属さなくなったobjectは、公開に失敗した受け取りの残りである。段階5aは、objectのfileを消さない（消すのは索引の行だけ。fileは段階5bが扱う）。

**受け入れ試験。** (i) 100個のblobを持つsourceを受け取った後、`import_session` と `root(kind='pending')` の行が残らず、`receive:<source>` のrootがすべてのobjectを持つ。(ii) 同じsourceを2回受け取っても、rootの行が増えない。(iii) blobを入れた後、公開の前にagentをSIGKILLし、再起動後に同じ受け取りが続き、終わると (i) と同じになる。(iv) 作業directoryを捨てると、そのtokenの行が消える。(v) 起動時に、記録の無い古い行が消え、公開済みのsourceの検証（`blob_verify`）が `ready` のままである。(vi) submoduleを持つsource（manifestが複数）で (i) が成り立つ。(vii) `incoming/<source>/` が、24時間進まなければ、16個に達していなくても消える。

**schema。** 変更は無い（tokenのIDは作業directoryのfileに書く）。wireの変更は無い。

### 5b: 受け取ったsourceとcloneの中身の削除

**今の挙動。** workerが受け取ったsource（`sources/manifests/`・`sources/objects/` と、`clones/<repo>` の中のrefとGit object）を消す処理は無い。受け取ったfileの数だけ増える。読み手は `blob_verify`（何が既にあるかの答え）と、attemptのworkspaceの準備である。受理は、sourceがstoreに無くても受け付け（`src/worker/agent.rs:2697`〜`2708`。artifactを宣言しないpayloadではsourceを見ない）、準備の時にsourceが無ければ、型の無い失敗（`NOT_FOUND`、`src/worker/machine.rs:202`〜`207`）でattemptを終える。

**前提。** 段階5a（受け取りのpending rootの持ち主と解放）が先に入っていること。失敗した受け取りが入れたblobは、5aの前は、blobごとのpending root（`src/source/store.rs:603`、`src/source/retention.rs:636` の `import_blob`）が持ち続ける。5aは、それを受け取りの作業directoryが持つtokenに束ね、作業directoryを捨てるときに解放する。5bは、次の2点を5aに求める。

- **tokenのrootの種類は `receipt` とする**（5aの実装は今 `kind='pending'`・`source` が空のrootを作る。`src/source/retention.rs:842`）。`receipt` のrootは、memberの行が持つものの全部である: 受け取りがblobを入れるのは `import_blob` だけで、objectの行とmemberの行を同じtransactionで書き、そのtransactionが、そのobjectのclaimが無いことを確かめる（`src/source/retention.rs:642`〜`670`）。したがって、「memberの分からないroot」（取り込み中の親のroot。中身をfileに書いてから登録する）を理由に全体を止める3つの確認（`drain_claim` の `src/source/retention/deletion.rs:334`、`retention_page` の `:566`、門の `closed` の `:44`、`src/source/retention.rs:1173`、`src/source/retention/vault_retention.rs:374`。`begin_import` と公開が見るのはclaimで、rootの種類ではない）は、`kind NOT IN ('abandoned','receipt')` に狭める。`receipt` のrootが持つobjectは、どれも `member` の行で守られる（`drain_claim` の `EXISTS(member WHERE object)`）。
- **tokenの解放は、先にrootを `abandoned` にしてから**、`release_member` でmemberを外す（25.4の放棄と同じ順）。他のrootが持たないobjectは、そこで（claim・内容の確認・unlinkの順で）消え、公開が済んだ受け取りのblobは `receive:<source>` のrootも持つので残る。解放は、下の (d) の1つの区切りとして、sourceの変更lockを待たずに排他で取って行い、取れなければtokenを作業directoryに記録したまま次のtickでやり直す（作業directoryは、解放が済んでから消す）。tokenのrootが `abandoned` になった後に途中で終わっても、残ったmemberは (d) の段0が外す。

したがって5bは `sources/objects/` の全走査をしない。5aより前の版が残したpending rootの行は、5aの起動時の処理が消すので、5bの時点では残っていない。

**storeは親と共有しない。** workerのsourceのstoreは `worker_state_dir/sources`（`src/service/bootstrap.rs:25`、`:333`、`src/worker/machine.rs:58`）で、親のstate（`sources/`）とは別のdirectoryである。同じmachineで親とworkerを動かしても、段階1・2の削除と5bは、別の `retention.db` と別の `mutation.lock` を扱う。設定で同じdirectoryを指した場合に備え、5bのpassは、そのstoreに親のroot（`kind` が `run`・`import`）があれば、何も消さずにpassを終え、eventを1回書く。

**原則: lockが基準である。** 受け取ったsourceを消すのは、sourceの変更lock（`sources/mutation.lock`。`src/source/retention.rs:267` の `guard`。storeに1つで、sourceごとではない）を排他で持っている間だけである。消す処理は、そのlockの下で `worker.db` を直接読み、参照を確かめる。`retention.db` に参照の行は書かない。段階2が親で提出と刈り取りを分けるのと同じ形である（25.5の段9）。

**claimを区切りの外へ持ち越さない。** 排他を持つ1つの区切りの中で書いたclaimは、同じ区切りの中で消し終える（`release_member` はclaimの直後に `drain_claim` を呼ぶ。`src/source/retention/deletion.rs:484`）。claimが残っている間は、`begin_import` も公開（`ensure_sdk_available`、`src/source/store.rs:744`）も、どのsourceについても断る（`src/source/retention.rs:799`、`:830`）ので、claimを持ち越すとstore全体が止まる。crashで残ったclaimは、次のpassの最初に消す（(d) の段0）。`drain_claim` が断ったclaim（内容が変わった等）は残る。残っている間は、passは他のものを消さず、受け取りと公開は送り直しになる。人が直すまでの間だけで、eventに理由を書く。

#### (a) 参照

- **参照の定義。** source `S` が参照されているとは、次のどちらかが成り立つことである。(1) `worker.db` に、どのメンバーのものでも、`state <> 'terminal'` で、`source_id` が `S` のattemptの行がある。(2) `receive:<S>` 以外のrootのgraphの行（`graph(root, source)`。受け取ったsourceが使うsubmoduleのsourceを名指す。`src/source/retention.rs:551`〜`576`）が `S` を名指す。(2) は、親のsourceが参照されているかに依らない: 親のsourceが在る間、子は消えない。親が消える（そのrootとgraphの行が消える）と、子は (1) と (c) の時刻だけで判定される。子は、自分の `used_ms` が古くても、親が在る間は残る。
- **索引。** `worker_attempt` には今、sourceの列が無い（`src/storage/schema.rs:313`〜`335`）。段階5bは、列 `worker_attempt.source_id` と部分索引 `worker_attempt(source_id) WHERE state <> 'terminal'` を足す（25.13）。migrationは、terminalでない行についてだけ `spec_json` から列を埋める。`graph` には `source` から引く索引が無い（主キーは `(root, source)`、`src/source/retention.rs:174`）ので、索引 `graph(source, root)` を足す。判定は、どちらも索引の検索1回である。
- **受理。** workerは、新しいattemptのsubmit（`worker.db` に行が無いもの。`fresh`、`src/worker/agent.rs:2682`〜`2684`）を受理するとき、sourceの変更lockを**共有で、待たずに**取り、sourceがstoreにあることの確認から、`worker.db` の受理のcommit（`submit_within`、`src/storage/worker.rs:780`）までの間、持ち続ける。取れなければ、何も書かずに `OVERLOADED`（新しい理由 `source_store`。送り直せる）で答える。lockを取る位置は、`submit_within` の外で、sourceに依る検査（`secret_paths`、`src/worker/agent.rs:2706`）より前である。既に行のあるattemptのsubmit（送り直し）は、今と同じく行を読んで答え、sourceを確かめない: そのattemptがterminalでなければ、sourceは (1) で参照されていて消えていない。
- **attemptのための読み手。** 受理されたattemptのための読み取り（準備の `manifest_in`、`src/worker/machine.rs:93`、`agent.rs:973`〜`987` から。CIのsupervisorの `store.manifest`、`src/supervisor/ci_task.rs:330`〜`334`。blobとclosureを読む `src/source/store.rs:465`・`:473`・`:531`・`:586`・`:804`）は、いずれも共有lockを待たずに取り（`guard(false)`、`try_lock_shared`、`src/source/retention.rs:280`〜`282`）、取れなければ型の無い失敗になる。5bは、これらを、共有lockを**待つ**取り方（10 msから倍にして50 msまでの間隔で取り直し、120秒で諦めて今と同じ失敗）に変える。消す処理が排他を持つのは、(d) の1つの区切り（object 1つの確認とunlink、またはsource 1つの切り離し）の間だけで、1つの区切りが読むのは1つのblobの2回分（`release_member` の確認と `drain_claim` の確かめ直し）までである。120秒は、受け取れる大きさのblob（`max_blob_bytes`、`src/worker/blob_policy.rs:29`）の2回分のhashより長い。受理は、agentのtickを止めないよう、待たずに `OVERLOADED` で答えるまま（上）とする。
- **安全の証明。** 消す処理は排他を持って `worker.db` を読み、受理は共有を持って「sourceがある」を確かめてからcommitする。排他と共有は同時に持てないので、どの受理も、(i) 消す処理の確認より前にcommitが済んでいる（確認がその行を見て消さない）か、(ii) 消す処理がlockを放した後に「sourceがある」を確かめる（manifestが消えていれば下の (b) で断る）か、のどちらかである。消す処理は、確認と同じ区切りの中でmanifestを消す（(d) の段3）ので、確認から後のどの受理も、manifestの無いsourceを見る。受理されたattemptがterminalになるまで、そのsourceは参照されている。準備の時（`src/worker/machine.rs:202`）にsourceが無いことは、この規則の下では起きない。起きたら（人がfileを消した等）、今と同じ型の無い失敗のままにする。

#### (b) 消えたsourceを名指すsubmit: 型つきの断り

- **「storeにある」の定義。** source `S` がstoreにあるとは、共有lockの下で、索引に `receive:<S>` のrootの行があり、その `source` が `S` であることである（fileの有無では判定しない。切り離しの後に、`m:<S>` のfileが確かめられずに残ることがある: 予算切れ・確かめられないleaf・古い `drop:` のrootとの共有。`src/source/retention/deletion.rs:275`〜`278`）。受理の確認、公開の時の (b) の `INPUT_MISSING`、受け取りの `published`（(h)1の(iii)）は、この同じ判定を使う。manifestの中身は、その後でfileから読む。
- **規則。** 新しいattemptのsubmitで、payloadの名指すsourceがstoreに無いものは、新しいcode `INPUT_MISSING` で断り、attemptの行を書かない（tombstoneも書かない）。断りには `source_id` を付ける。artifactの有無に依らず、新しいattemptの受理は必ずsourceの有無を確かめる（今はartifactを宣言しないpayloadではsourceを読まない。第13章の文が変わる。25.14）。
- **親の扱い。** 親は `INPUT_MISSING` を受けたら、その接続の `ready` から、そのworkerの、そのsourceの行を外す（`InputGate::retain`、`src/coordinator/input_gate.rs:108`、接続ごとの `src/coordinator/link.rs:193`）。attemptは配置済みのまま、送っていない扱い（`Sent::NotTaken`）にする。配送し直しは、residentの配送のloopが行う: loopは、そのworkerの、terminalでないattemptのinputを頁で読み（`input_page(live_cursor, 64, Some(worker))`、`src/service/resident/delivery.rs:49`〜`56`）、`ready` に無いものを配送し、確かめた後に `ready` に入れる（`publish_verified`、`delivery.rs:224`）。
- **`ready` が戻るまで送らない。** 配置の後の送り直し（`Step::Submit`、`src/coordinator/mod.rs:1446`。`send_placed`、`:2613`）は、今は `ready` を確かめずにtickごとに送る。5bでは、`INPUT_MISSING` を受けたattemptを、そのworkerの `ready` に同じrun・source・`manifest_digest` が戻るまで、どちらの経路でも送らない（`Sent::Withheld`）。印は、親のprocessのmemoryに、attemptごとに1つ（`(attempt, source, digest)` と、戻った `ready` の世代）持つ。attemptがterminalになるか配置が外れると消える。親が再起動すると印は消えるが、再起動の後の最初の送り直しは、断られれば同じ印をまた付けるだけである。
- **回数。** `INPUT_MISSING` は受理されていないので、taskのretryの回数に数えない。数えるのは、新しい `ready`（印を付けた後に `ready` が戻ったこと）の後の送り直しへの断りだけで、それが3回続いたら、親はそのattemptをretireし（理由 `input_missing`）、taskのretryの規則で次のattemptを作る（このときは回数に数える）。回数は上の印と同じ所（親のmemory）に持つ。ふつうの送り直しの間の断りは数えない。公開の時の `INPUT_MISSING`（blobの送り）は配送の失敗なので、配送のloopの `fail_preference`（`src/service/resident/delivery.rs:218`、`src/coordinator/link.rs:129`）を通り、そのrunの選んだ経路をその世代について失敗とし、次の配送は別の経路の候補から選ぶ。workerがsourceを受け取ると、公開が最後に使った時刻を記録する（(c)）ので、配送し直した直後に期限で消えることは無く、3回続くのは、量の上限で毎回押し出されるときだけである。
- **前提の確認（`ready` が先）。** 最初の配置は、そのworkerの `ready` に、同じrun・source・`manifest_digest` があるときだけ行う（`src/coordinator/mod.rs:2053`〜`2068`）。`ready` を持たない（gateの無い）linkは、製品の経路に無い（`ResolvedConfig::attach_worker`、`src/config/runtime.rs:44`、を呼ぶのはtestだけ）。
- **古い親。** wireの機能 `input_missing_v1` を交渉した親にだけ `INPUT_MISSING` で断る。交渉していない親からのsubmitは、今と同じく受理し、準備の時の `NOT_FOUND` で終える（共有lockの規則は同じに守るので、受理の後に消えることは無い）。
- **`blob_verify` と消す処理。** `blob_verify` は何も変更せず、lockも持たない（第11章）。`present` と答えたblobが、公開の前に消されることがある。公開（`publish_checked_root`、`src/source/store.rs:746`）は、manifestの名指すblobが無いと `corrupt` で失敗し、今は受け取り全体を拒否して作業directoryを捨てる（`src/worker/blobs.rs:1437`〜`1459`）。段階5bでは、公開の時に、名指すblobが「無い」か「削除のclaim中」のときは拒否せず、作業directoryを残したまま `INPUT_MISSING`（`missing` に足りないblobを入れる）で答える。親は、そのsourceの配送を `blob_verify` からやり直す。claimは区切りの外へ残らない（上）ので、「claim中」は、crashの後と、`drain_claim` が断った後にだけ起きる。blobの大きさが違う等、内容の誤りは今と同じに拒否する。作業directoryを残すので、その受け取りが展開で付けたrefも残る。今の `made` はmemoryにだけあり（`src/worker/blobs.rs:143`〜`148`）、展開し直しは既に在るrefを `made` に入れない（`:1279`〜`1282`）ので、捨てる時の手がかりにならない。作業directoryを捨てる処理（5aの24時間と、受け取りの拒否）は、作業directoryのmanifestのsource IDから `snapshots/<id>` と `index/<id>` を求め、`clone_ref` の行の無いものを、そのcloneの取り込みlockの下で消す。

#### (c) 量: storeを上限で抑え、sourceごとには数えない

- **bytes。** `retention.db` の `object` の行に、列 `bytes` を足す（25.13）。合計を `meta` の1行（`store_bytes`）に持つ。`bytes` は、fileを置いたことを確かめたときに書く（受け取りは、fileを置いた後の同じ取り込みの中で。登録は、登録の時のstatで）。`bytes` が空から値になる更新（`changes() = 1`）のときだけ、同じtransactionで `store_bytes` に足す。行はあってfileの無いobject（上の段0の (ii)）は、`bytes` が空のままで、合計に入らない。減らすのは、objectのfileを消し終えたtransactionである: `drain_claim` の完了のtransaction（`src/source/retention/deletion.rs:397`〜`408`）でobjectの行と、そのclaimの行（`source_delete_claim`。今は `done=1` にして残す、`:402`）を消して、`bytes` を引く（今はobjectの行を残す。消した後に同じobjectが入れば、`INSERT` が行を入れ直して足す）。放棄の処理（`abandon.rs:482`）とscratchの削除（`scratch.rs:183`）がobjectの行を消すときも、同じtransactionで引く。`publication` の行は、fileを消したobjectについては残す（世代の番号が、古いclaimと古い読み手を断る。25.9）。ただし、fileの無いmemberの片付け（(d) の段0の (ii)）と、claimの行を消すことで `NOT EXISTS(claim)` が真になる親の放棄（`abandon.rs:487`）と予約の解除（`scratch.rs:181`）は、`publication` の行も消し、次に入れると世代は1から始まる（`src/source/retention.rs:660`）。これは安全である: 世代を持つ読み手は共有lockの下で読み、行を消すのは排他の下なので、行を消した時点で古い世代を持つ読み手もclaimも無い。この規則は親のstoreにも同じに効く（同じcodeである）。既に在る行は、migrationがmarkerを置き、各passが、`bytes` の空いた行を1,000行までstatして埋める。fileが無い行は空のまま残す（後でfileを置いたときに数える）。全部埋まるまで、量による削除（下の2）は行わず、期限による削除（下の1）だけを行う。
- **最後に使った時刻。** `root` の行に列 `used_ms` を足す（`received` のrootだけが使う）。記録するのは、公開（`publish_checked_root`。workerの公開はこの関数を通り、`finish_import` を呼ばない。`src/source/store.rs:713`〜`723`、`743`〜`745`）、attemptの受理（共有lockの下、`worker.db` のcommitの後）、attemptのterminal、の3か所である。`blob_verify` では記録しない。受理とterminalでの記録は、共有lockが取れなければ記録を省く（時刻が古く見えるだけで、参照の判定は `worker.db` で行うので安全は変わらない）。submoduleのsourceは、親のgraphの行が守る（(a) の (2)）ので、子のrootの時刻は記録しない。
- **規則。** 参照されていない受け取ったsourceのうち、
  1. 最後に使ってから14日を過ぎたものを消す。
  2. `store_bytes` が20 GiBを超えている間は、最後に使った時刻の古い順に消す。
  数値は、setup templateと同じ14日・20 GiBである。1の判定は時計の規則（第23章）に従い、時計がHoldの間は行わない。2は順序にだけ時刻を使い、Holdの間も行う。共有のobjectは、最後のmemberと一緒にだけ消える（`release_member`）。上限を超えたままでも、参照されているsourceは消さない。

#### (d) 消す手順と、1回の上限

workerのagentのtick（既存の監視の周期）で、60秒に1回まで行う。ただし、段0の残りがあるか、`store_bytes` が上限を超えている間は、passが終わったらすぐ次のpassを始める（区切りの間で排他を放し、100 ms置いてから次の区切りを取る。下の読み手の取り直しの間隔は50 msまでなので、待っている読み手が先に取れる）。試用中（24.8）は行わない。1つのpassは、いくつかの**区切り**から成る。区切りごとに、sourceの変更lockを**待たずに**排他で取り、終えたら放す。取れなければ（受理・受け取り・公開・読み取りの最中）、そのpassを終える。

0. **残りの片付け。** 区切りの最初に、(i) 終わっていないclaim（`source_delete_claim` の `done=0`）があれば、`drain_claim` で消す。(ii) `kind='abandoned'` のroot（切り離したsourceと、解放の途中のtoken）にmemberが残っていれば、passの持つcursor（`(root, object)`）より後のmemberを順に扱う。他のrootも持つmemberは、memberの行だけを外す（1つの区切りで1,000行まで）。持たないmemberは、1つの区切りで、区切りの64 MiBの内で複数、claim・確認・unlinkまで行う（64 MiBより大きいものは、下の「1回の上限」のとおり排他の外でhashする）。fileが無い（自分のpathを、祖先の確認つきで開いて `ENOENT`）memberは、25.4の (b) の `destination_absent` と同じく（`src/source/retention/deletion/abandon.rs:474`〜`497`）、member・object・publicationの行を消す（`import_blob` は行をfileより先に書き、hashの不一致ではfileを置かずに行を残す。`src/source/store.rs:612`〜`651`）。確かめられない（`Unproven`）memberは残し、cursorがその先へ進む。cursorは `meta` の `drop_cursor` に持ち、passをまたいで進み、末尾で先頭に戻る（確かめられないmemberが先頭に溜まっても、後ろが進む）。memberが無くなったrootは、その行を消す。(i)・(ii) を行った区切りは、そこで終える。1つのpassの区切りは64個までで、超えればpassを終える。段0は区切りの半分（32個）までとし、残りを段2以降に充てる。
1. **門**（段0を含む、claimを書くどの区切りの前にも、tokenの解放の前にも確かめる）。`source` が空で `kind NOT IN ('abandoned','receipt')` のroot（登録の途中の `receive:<S>` は `pending` で `source` が空。`src/source/retention.rs:442`、`498`）、または `graph.done=0` の行があれば、何も消さずにpassを終える（`retention_page` の `unknown` と同じ。`src/source/retention/deletion.rs:566`）。これらは、途中で終わった公開が残すもので、その受け取りの続きか、起動の時の片付け（(h) の1）が片付ける。門が閉じていればclaimを書かないので、claimが残ってstoreが止まることは無い。
2. **候補。** `root(kind='received')` のうち、他のrootのgraphに名指されないもの（`NOT EXISTS(SELECT 1 FROM graph WHERE source=r.source AND root<>r.id)`、索引 `graph(source, root)`）から、`used_ms` の古い順に索引で1つ選ぶ（索引 `root(kind, used_ms)`。25.13）。submoduleのsourceは、時刻を記録しないので常に古いが、親が在る間は候補に出ないので、32件の上限を埋めない。(c) の規則に当たらなければpassを終える。
3. **切り離し**（1つのtransaction）。(a) の参照を `worker.db` と `graph` の索引で確かめ直す。参照されていれば、次の候補へ（同じ区切りのまま2から。1つの区切りで確かめる候補は32件まで）。参照されていなければ、同じtransactionで、`receive:<S>` のmemberを、新しいroot `drop:<S>:<ulid>`（`kind='abandoned'`、`source` は空）へ移し、`receive:<S>` の行とそのgraphの行を消し、そのrootの `clone_ref` の行（(e)）を消す。続けて、manifest `m:<S>` のmemberを (ii) と同じく外す（他のrootが持たなければ、claim・確認・unlinkを、この区切りの中で行う）。この区切りの後、`S` のmanifestは無く、受理は `INPUT_MISSING` で断る。`S` のblobは `drop:` のrootが持ち、`blob_verify` は `present` と答えうるが、公開の時に無くなっていれば (b) で `INPUT_MISSING` になる。`S` を受け取り直すと、新しい `receive:<S>` を作り、`register_held` が全部のmemberを登録し直す（`complete` は行の無いrootでは偽、`src/source/retention.rs:413`〜`424`）。同じblobを `drop:` も持っていれば、`drop:` のmemberを外すだけで、blobは残る。
4. **cloneのref**（(e)）。3で `clone_ref` の行が無くなった `(clone, source)` のrefを消す。この区切りの中で、cloneの取り込みlockが取れなければ、そのrefを `meta` の `ref_drop:<clone>:<source>` に残し、次の区切りの段0の後に試す（行の数は、切り離したsourceのref数で頭打ち）。試す前に `(clone, source)` の `clone_ref` の行が無いことを確かめ直し、在れば（受け取り直された）refを消さずにmetaの行だけを消す。
5. 排他を放す。次の区切りへ。

**1回の上限。** 1つの区切りで、切り離すsourceは32件まで、排他の下で検証する内容は64 MiBまで。64 MiBより大きいblobは、排他の下ではclaimを書くところまでを行い、内容の確認（hash）は排他を放してから行い、排他を取り直して、leafのidentityと祖先を確かめ直してからunlinkする。claimの間は、そのobjectを書く経路がどれも断る（`import_blob` と登録の `check_publication`、`src/source/retention.rs:674`〜`681`。読み手の `ensure_blob_available`）ので、排他を放している間にfileを置き換えるbelt自身の書き手は無い。人の書き換えは、取り直した後のidentityの確かめが断る。したがって、排他を持つ時間は、1つの区切りで、64 MiBの読み取りとhash、`git update-ref` 数回、transaction数回である。claimの再実行（`drain_claim` の確かめ直し、`src/source/retention/deletion.rs:355`〜`356`）も、同じく64 MiBより大きいものを排他の外でhashする（今は予算に収まらないclaimを断り、予算が区切りごとに64 MiBに戻る、`:78`、`:579`、ので、64 MiBを超えるclaimが残ると、段0の (i) が毎回それで止まる）。量の速さは、排他の外のhashを含めて、diskの読み取りの速さで決まる（区切りの間の100 msを除く）。その間の受理は `OVERLOADED` で断られ、送り直される。attemptのための読み手は待つ（(a)）。

#### (e) cloneのref、gc、cloneのdirectory

- **どのcloneにrefを付けたかの記録。** 受け取りは、sourceのsnapshotとindexのrefを、そのsourceのmanifestの `repository_id` のcloneではなく、受け取った（親の）sourceのcloneに付ける。submoduleのsourceのrefも親のcloneに付く（`ObjectsRepo::import_refs`、`src/source/store.rs:222`〜`241`、呼び出しは `src/worker/blobs.rs:1279`）。そこで、公開（`publish_checked_root`）は、付けたrefごとに `retention.db` の `clone_ref(clone, source, root)` の行を、rootの登録と同じtransactionで書く（`root` は `receive:<P>`）。同じrepositoryの2つのsourceが同じsubmoduleのrefを共有するときは、行が2つになる。refを消すのは、`(clone, source)` の行が1つも無くなったときだけである。行の数は、graphの行の数で頭打ちになる。5bの前に受け取ったsourceには行が無い。migrationは、`received` のrootのgraphの行ごとに、親のmanifestの `repository_id` のcloneについて行を書く（今のcodeが付ける先と同じ）。
- **ref。** 段の4で、同じ排他の下で、さらにそのcloneの取り込みlock（`clones/<name>.import.lock`、`src/workspace/mod.rs:202`。bundleの展開はこれを待って取る。`src/worker/blobs.rs:1186`）を待たずに排他で取り、local-ci 0.19.0の `remove_imported_refs`（`refs/local-ci/snapshots/<S>` と `refs/local-ci/index/<S>` を消す）を呼ぶ。beltがworkerの受け取りで付けるrefは、この2つだけである（`refs/local-ci/heads/*` は付けない）。cloneは、`repository_id` から `clones/r-<sha256の先頭8 byteの16進>`（`src/workspace/plan.rs:33`〜`37`）として引く。取り込みlockの下で消すので、同じsourceを取り込み直す展開と、refの付け外しが入れ違うことは無い。
- **gc。** Git objectは、`git gc --prune=14.days.ago` で回収する。sourceのlockの外で、refを最後に消してから15日の間のcloneについて、1つのcloneに1日1回まで（`meta` の行 `gc:<name>` に、最後に行った時刻と、最後にrefを消した時刻から15日後の期限を持つ。期限を過ぎたら行を消す。行の数はcloneの数で頭打ち）。gcの間は、そのcloneの取り込みlockを待たずに排他で持つ（取れなければ、そのcloneのgcを次のpassへ回す）。展開は、bundleの前提のcommitがrefから届くことを確かめてから展開する（`holds_base` は `rev-list <前提> --not --all` が空であることを見る。`src/source/store.rs:315`〜`325`）ので、前提のobjectはgcが消さない。取り込みlockは、展開がpackとrefを書く間にgcが到達の判定をすることを除く。`--prune=now` は使わない（workspaceはcloneのobject storeを共有し、利用者のcommandがbeltのlockを持たずに、refを付ける前のobjectを書く。Gitの14日の猶予と、既に在るobjectを書き直すときの時刻の更新が、それを守る）。Gitの猶予はobjectのfileの時刻（mtime）で数える。refを消した後の最初のgcで、14日より古いobjectは消え、新しいものは書かれてから最長14日と1日残る。`git gc` は `-c gc.packRefs=false` で呼ぶ（refの数は小さく、利用者のcommandがworkspaceでrefを更新するときのlockと競わない）。
- **cloneのdirectoryは消さない。** `<clone>/.git` の中の `fast-worktree-registration.lock` は、fast-worktree 0.4.0以降の版が、beltのlockを持たずに、pathを照合し直さずに開く（fast-worktree 0.7.0の `src/git.rs:128`〜`131`。belt自身のcloneのlockも、取得の後にinodeを照合しない。`src/workspace/mod.rs:863`、`882`）ので、25.1の (A) も (B) も示せない。cloneの数は、受け取ったrepositoryの種類（`repository_id`）の数で頭打ちになる。今の `remove_clone_if_empty`（`src/workspace/mod.rs:243`）の扱いは25.9に置く。

#### (f) `GuardedObjectStore::put_bytes` を消す

`GuardedObjectStore::put_bytes`（`src/source/store.rs:384`）は製品の呼び出し元が無い。段階5bの実装の変更でこの関数を消す。これを書き込みのfixtureに使うtestは、別の書き手（`SourceStore` の予約つきの取り込みか、`ingest_blob_with_token`）に置き換え、assertは減らさない: `fixture_run_fence_does_not_change_content_identity_or_new_publication_generation`（`src/source/retention.rs`）、`opened_reader_keeps_source_shared_guard_after_async_waiter_cancellation`、`guarded_sdk_view_retains_shared_ownership_and_claim_fences_its_callback`、`source_upload_opens_verified_descriptors_without_links_or_whole_reads`（`tests/it/source_capture.rs`）、`measured_tests::upload`（`src/service/forward.rs`。`contiguous_chunks_with_size_probes_are_complete_but_resume_is_unknown` が使う）。

#### (h) 既存のcodeの変更（5bの実装に含める）

5bの証明は、次の変更に依る。

1. **空の `receive:<S>`。** 公開（`publish_checked_root`、`src/source/store.rs:746`）は、blobとsubmoduleの確認（`:752`〜`776`）の前に `pending(root)` でrootの行を作り（`:751`）、失敗しても残す。`register_step` はrootとgraph（`done=0`）の行をtransactionの前に書く（`src/source/retention.rs:497`〜`504`）。これを、(i) `pending(root)` を確認の後へ移し（同じ共有lockの下）、(ii) agentの起動の時（`agent.lock` の下で、`mutation.lock` を120秒まで待って排他で取り、受け取りの前。取れなければ片付けを飛ばし、passの最初の区切りで行う。`source` の空のpendingのrootが在る間は門が閉じているので、片付けが済むまで何も消えない。親のrootがあれば何もしない。上の「storeは親と共有しない」と同じ確かめ）に、`source` が空の `receive:%` のrootを片付ける: manifestのfileが確かめられれば登録を終え、確かめられなければ `abandoned` にしてgraphの行を消す（memberは段0が外す）、に変える。(iii) 子のmanifestはfileが索引より先に書かれる（`src/source/store.rs:785`〜`793`）ので、`published(id)`（`src/worker/blobs.rs:1431`）は、fileではなく `receive:<id>` のrootの `source` が埋まっていることで判定する。
2. **受け取りの読み手。** 受け取りの経路（`published` から `manifest`、`src/worker/blobs.rs:564`。`begin_import`、公開の `:1417`・`:1426`・`:1440`）は、共有lockを待たずに取り、取れないと型の無い `internal("source store")` で答え、親の配送が失敗する（`src/service/forward.rs:551`）。5bでは、`complete()`（`src/worker/blobs.rs:1309`〜）の最初、sourceのlockを取った直後に、待つ取り方で共有lockを1つ取り、`published`（`:1318`）・blobの取り込み（`:1415`〜`1426`、`begin_import` も中で同じprocessの別のfdで共有を取る）・`leaf_first` の公開のloop（`:1428`〜）の全体で持ち、`discard()` の前に放す（`discard()` の中の5aのtokenの解放は、別のfdで排他を待たずに取るので、持ったままだと必ず取れない）。lockの順は、sourceのlock、共有lock、cloneの取り込みlock（待つ）である。loopの間は区切りが入らないので、`published` で飛ばした子が、親の公開の前に消えることは無い。公開の時に子のmanifestが無ければ（loopの前に消えた）、`corrupt` ではなく `INPUT_MISSING` で答える。
3. **claimの行。** `drain_claim` の完了のtransactionは、claimの行を消す（(c)）。世代は `publication.incarnation` が持つ。この変更は親のstoreにも効く。
4. **内容の確認。** `examine` はblobを丸ごとmemoryへ読む（`read_to_end`、`src/source/retention/deletion.rs:280`〜`281`。`max_blob_bytes` の既定は4 GiB、`src/worker/blob_policy.rs:58`）。5bでは、読みながらhashする形に変える。
5. **旧い版のmanifest。** workerのstoreは `legacy_registration` を `pending` のまま持ち（`src/source/retention.rs:188`）、5aより前に受け取ったmanifestには `m:` の行もrootも無い。5bのmigrationの後、各passは、`m:` の行の無いmanifestのfileを32件まで `receive:<id>`（`used_ms` は今）として登録し、migrationの最初の1回だけ `sources/manifests/` を並べ、`m:` の行の無い名前を表 `legacy_queue(name)` に入れる（行の数は、その時のmanifestの数）。各passは、そこから32件ずつ登録して行を消す。登録できないmanifest（子のmanifestが無い等）は、登録できた分のmemberを持つrootを `abandoned`（`source` は空）にし、`m:<id>` のmemberを外して、次へ進む。`done` になるまで何も消さないので、この間に消えるものは無い。そのsourceは、(b) の判定で無いと読まれ、`INPUT_MISSING` で配送し直される。表が空になったら `legacy_registration` を `done` にし、表を消す。`legacy_registration` が `pending` の間は、(b) の判定で、`m:` の行の無いmanifestのfileも「ある」とみなす（`done` になるまで何も消さず、切り離しも無いので、切り離しの後に残ったfileを「ある」と読むことは生じない）。親のrootがあれば行わない。passは、`done` になるまで何も消さない（門の条件に足す）。
6. **どのthreadで動くか。** 消すpassは、agentのtick（`src/worker/agent.rs:2167`）から `spawn_blocking` で別のthreadに出し、tickを止めない。(a) の120秒の待ちも、準備・supervisor・受け取りのそれぞれのblocking threadで起き、agentのloopを止めない。消すpassは、agentの中の実行中の印（tickの `ticking`、`src/worker/agent.rs:2167`〜`2172`、はtickが戻ると外れるので、別に持つ）で、同時に1つだけ走る。
7. **terminalの後の読み手。** terminalの後もsourceを読む経路がある: 準備の `prepare` のthreadは、待ち手が時間切れで諦めた後も読み続ける（`src/worker/agent.rs:1269`〜`1301`）。setupのbuilderのthreadは、attemptの解放（`src/setup/builder.rs:1021`〜`1038`）で止めの印を立てるだけでjoinせず、workspaceを作り続ける（`:1070`〜`1087`、`:1379`）。そこでagentは、process内の印 `pins: HashMap<source, count>` を持つ（消すpassと同じprocess）。`prepare` とbuilderのthreadは、共有lockの下で `S` がstoreにあることを確かめてから印を1つ足し、threadの終わりで外す（guardのdrop）。印を足す時に `S` が無ければ、threadは今の型の無い失敗で終わる（attemptがterminalの後なので、結果は使われない）。setupのbuilderの補助processは、workspaceだけを読み、sourceのstoreを開かない（`src/setup/` に `SourceStore` を開くcodeは無い）。段3は、印のある `S` を参照されているものとして扱う。準備の全体で共有lockを持ち続けることはしない（排他が取れなくなる）。印はprocessの中だけにあり、agentが終われば、threadも一緒に終わる。builderの結果の記憶は、toolの導入の失敗だけを、toolの版とhostを鍵に短い時間だけ持つ（`failed_tools`、`src/setup/builder.rs:291`、`:666`〜`680`、`policy::failure_held`）。sourceを鍵にしないので、受け取り直した後に古い結果を使い回すことは無い。
8. **受け取りの間の、carryのref。** 2つの作業directoryが同じcloneで同じsubmoduleのsnapshotを持つとき、後の展開は既に在るrefを `made` に入れない（`src/worker/blobs.rs:1279`〜`1282`）ので、前の作業directoryを捨てるとrefが消え、後の公開の `check_snapshot`（`src/source/store.rs:170`〜`183`）が `corrupt` で受け取りを拒否する（`blobs.rs:1442`〜`1459`）。5bでは、bundleの無い経路も、`commits_here`（`src/worker/blobs.rs:1391`）の前にcloneの取り込みlockを待って取り、`complete()` の終わりまで持つ（bundleの経路と同じ。`:1186`、`:1240`。lockの順は、sourceのlock、共有lock、取り込みlockのまま）。作業directoryを捨てる処理もこのlockの下でrefを消すので、公開の途中でrefが消えることは無い。そのうえで、公開の時に運ばれたmanifestのsnapshotのrefが無い（人が消した）ときは、拒否せず `INPUT_MISSING` で答え、親が配送し直す。

#### (g) 受け入れ試験

1. 受理と削除の競合: 消す処理が排他を持つ間の新しいsubmitは `OVERLOADED` で断られ、行が無い。消す処理が切り離した後に届いたsubmitが `INPUT_MISSING` で断られ、attemptの行が残らない（共有lockの取得を外すと、sourceの無いattemptが受理されて失敗する）。
2. 受理した後のsourceは、attemptがterminalになるまで、期限と量のどちらでも消えない（`worker_attempt.source_id` の確認を外すと消える）。
3. 他のメンバーの、terminalでないattemptが名指すsourceも消えない。
4. submoduleのsource: 親のsourceが在る間、子は、自分の `used_ms` が14日より古くても消えない（親を繰り返し使い、子の時刻が古いまま）。親が消えた次の判定で、子が消える（graphの (2) の確認を外すと、親が在るのに子が消える）。
5. `INPUT_MISSING` を受けた親が、そのworkerの `ready` を外し、`ready` が戻るまで、tickが何回進んでも同じattemptを送らない（送り直しの経路の両方で）。配送のloopが配送し直して `ready` が戻った後に送り直し、受理される。taskのretryの回数は増えない。`ready` が戻るたびに断られることが3回続くとretireされ、回数が1増える。`ready` が戻らないまま何tick進んでもretireされない。
6. `input_missing_v1` を交渉しない親には、今と同じく受理して `NOT_FOUND` で終える。
7. `blob_verify` と削除の競合: `present` と答えたblobが公開の前に消されると、公開は `INPUT_MISSING` で答え、作業directoryと `made` のrefが残り、`blob_verify` からやり直した配送で公開される。作業directoryを捨てると `made` のrefが消え、`clone_ref` が名指すrefは残る。
8. 取り込み直しと、refの削除の競合: 取り込みlockを持つ展開の間、refの削除はそのrefを次の区切りに回す。展開と公開の後、`refs/local-ci/snapshots/<S>` が残り、workspaceが作れる（取り込みlockの取得を外すと、refが消えて準備が失敗する）。
9. workspaceがobjectを書いている間のgc: refの無いobjectが、`git gc --prune=14.days.ago` の後も残り、そのcommitにrefを付けられる。`--prune=now` に変えると失敗する。展開が取り込みlockを持つ間はgcを行わず、次のpassで行う（取り込みlockの取得を外すと、gcが展開と同時に走る）。
10. 期限: 最後に使ってから14日を過ぎた、参照の無いsourceが消える。13日のものは残る。時計がHoldの間は消えない。
11. 量: 合計が20 GiB（試験では小さい値）を超えると、古い順に、参照の無いsourceだけが消え、上限を下回ったところで止まる。共有のobjectは最後のsourceと一緒にだけ消える。
12. 最後に使った時刻: 公開・受理・terminalで更新され、`blob_verify` では変わらない。
13. bytes: migrationの後、各passが1,000行まで埋め、fileの無い行には0を書き、埋まるまで量による削除をしない。sourceを消して同じ内容を受け取り直した後、`store_bytes` が、在るfileの大きさの合計と一致する（drainでの引き算か、`changes() = 1` の条件を外すと一致しない）。放棄とscratchの削除の後も一致する。
14. 1回の上限: 33件の候補で、1 passは32件で止まる。64 MiBを超える量は次のpassに回り、passの間にclaimが残らない。64 MiBより大きいblob（試験では予算を小さくして、予算の半分より大きいblob）が、1 passに1つ消える（最初の1つの例外を外すと、何passでも消えない）。
15. 区切りの間の読み手: 消す処理が排他を持つ間に、別の（参照されている）sourceの準備とCIのsupervisorのmanifestの読み取りが、失敗せずに待って読める（待つ取り方を外すと型の無い失敗になる）。
16. 受け取りが進行中の間の削除: 別のsourceの受け取り（`receipt` のroot）と、`INPUT_MISSING` の後に残した作業directoryがある間も、5bのpassはsourceを消し、passの後に `begin_import` と公開が断られない。tokenの解放が、自分のrootのblobを消し終える（`receipt` を門と `drain_claim` の確認から除くのを外すと、passが何も消さないか、claimが残って `begin_import` が断られる）。
17. 一部だけ消したsourceの受け取り直し: sourceの切り離しの後、そのblobの一部だけが消えた状態で、同じsourceを受け取り直すと、新しい `receive:<S>` が全部のblobを持ち、その後のpassが `drop:` のrootを片付けても、`S` のblobが全部残り、workspaceが作れる（切り離しをやめて `receive:<S>` を残すと、受け取り直したblobがtokenの解放で消える）。
18. submoduleのref: 親のcloneに付いた子のsnapshotのrefが、`clone_ref` の行が無くなったときに、親のcloneから消える。同じsubmoduleを使う別の親が在る間は残る。
19. cloneのdirectoryは、どの削除の後も残る。gcは1つのcloneに1日1回まで。
20. 失敗した受け取りの残り: 公開の前に拒否された受け取りが入れたblobは、作業directoryを捨てるときのtokenの解放で消え、他の受け取りや公開済みのsourceも持つblobは残る。解放の時に排他が取れなければ、tokenと作業directoryが残り、次のtickで消える（`sources/objects/` を走査しない）。
21. crashの後: 区切りの中のclaimの後にkillすると、次のpassの段0が消し終え、その後は `begin_import` が通る。
22. 親と同じdirectoryを指したstoreでは、5bは何も消さない。
23. 試用中は何も消さない。
24. `put_bytes` を消した後、(f) の5つのtestが同じassertで通る。
25. 公開の確認で失敗した受け取りが、空の `receive:<S>` を残さない。crashで残ったものは起動の時に片付き、その後のtokenの解放でclaimが残らず、公開が断られない（門をclaimの前に確かめるのを外すと、claimが残って公開が断られ続ける）。
26. hashの不一致で行だけ残ったmemberを持つtokenの解放が、1 passの中で終わり（区切りは64個まで）、行が消え、`store_bytes` が増えない。
27. 親に名指された子が33件以上ある間も、名指されない古いsourceが量の上限で消える。
28. 受け取りの公開のloopの間に区切りが入らず、飛ばした子が消えない。loopの前に子が消えていれば `INPUT_MISSING` で答える。
29. 完了したclaimの行が残らない。受け取り直した `S` のrefを、残っていた `ref_drop` が消さない。作業directoryを捨てると、manifestのIDから求めたrefのうち `clone_ref` の無いものが消える。
30. 共有のmemberだけを外す区切りが1,000行ずつ進み、4 GiBのblobの確認がmemoryに丸ごと読まない。
31. 5aより前に受け取ったmanifestは、登録が終わるまで、それと共有するblobが消えない。
32. 段3で `m:<S>` のfileが確かめられずに残っても、その後の新しいsubmitは `INPUT_MISSING` で断られる（fileで判定すると受理されて準備で失敗する）。
33. 準備の待ち手が時間切れでattemptがterminalになっても、`prepare` のthreadが終わるまで `S` は切り離されない。builderのthreadも同じ（印を外すと、threadの読み取りが失敗する）。
34. 消す処理が排他を持つ間に始まった受け取りが、型の無い失敗にならず、待って終わる。受け取りの拒否の後のtokenの解放が、共有lockを放した後に排他を取れる。
35. 親と同じdirectoryを指したstoreでは、起動の片付けと旧いmanifestの登録が何もしない。
36. 2つの受け取りが同じsubmoduleのrefを共有し、前の作業directoryを捨てた後、後の公開が `INPUT_MISSING` で答え、配送し直しで公開される。
37. 確かめられないmemberが64個以上先頭にあっても、passをまたいで後ろのmemberが消える。登録できない旧いmanifestがあっても `legacy_registration` が `done` になる。
38. 消す処理のpassが重ならない。refを消してから14日以内に書かれたobjectが、期限までのgcで回収される。
39. 64 MiBより大きいblobの削除を、claimを書いた後に止めても、次のpassで消し終わり、その後の削除と `begin_import` が止まらない（再実行に大きさの扱いが無いと、段0が毎回止まる）。
40. 登録できない旧いmanifestのsourceへのsubmitが `INPUT_MISSING` になり、配送し直しで公開される。登録が終わる前の旧いsourceへのsubmitは受理される。
41. 段0の残りか上限超えがある間、passが続けて走り、その間に待つ読み手が120秒より十分短く取れる。64 MiBより大きいblobのhashの間、排他を持たない。
42. 起動の時に排他が120秒取れなくても起動が進み、片付けが最初の区切りで行われる。

## 25.9 段階5bで閉じなかったこと

段階5bの設計メモ（以前のこの節）の9つは、25.8の5bで閉じた（1: `received` のrootを起点に、`release_member` でBの保護のまま消す。2: 型つきの断り `INPUT_MISSING`。3: 受理は共有lockを待たずに取り、削除は排他を待たずに区切りごとに取る。4: local-ci 0.19.0の `remove_imported_refs` と `clone_ref`。5: Gitの猶予つきのgcだけを使う。6: cloneのdirectoryは消さない。7: storeの合計を上限で抑える。8: `put_bytes` を消す。9: 5aのtokenを `receipt` にし、解放を `release_member` にする）。残るのは次である。

- **受け取りを拒否したときの、空のcloneの削除。** 今の `remove_clone_if_empty`（`src/workspace/mod.rs:243`。`src/worker/blobs.rs:1250`、`1457` から呼ぶ）は、その受け取りが新しく作ったcloneにrefが1つも無ければ、`clones/<name>.lock` の下でdirectoryごと消す。cloneの中の `fast-worktree-registration.lock` と `belt-workspace.*` について、25.1の (A) も (B) も示していない。ただし、危険は小さい: 呼び出し元の2つは、どちらも取り込みlockを持ち、この取り込みが作ったcloneだけを対象にする。refの無いcloneは、workspaceの作成が `fw.add` の前の `has_commit` で失敗する（`src/workspace/mod.rs:345`）ので、beltはその中のlockを開かない。残るのは、fast-worktreeを単独で動かすprocessが、作ったばかりのcloneを開くことだけである。段階5bの「cloneのdirectoryは消さない」の例外として残すか、消すのをやめるかを決める。決めるまで、今の挙動のままとする。
- **`publication` の行。** objectの行は、fileを消すときに消す（(c)）が、`publication` の行は、世代の番号のために残す。行の数は、そのstoreに入ったことのある、内容の違うobjectの数で、1行は数十byteである。消すには、古い世代を持つ読み手とclaimが残っていないことを示す必要がある。

## 25.10 場所ごとの一覧

列は、もの / path / 読み手 / 今の挙動 / 規則（残す量・期間と、消す出来事） / 段階 / 使われていない証明、である。「段階」は、その行の「規則」を成り立たせる段階で、「既存」は今の実装が既に満たしていることを表す。段階が入るまでは「今の挙動」が正である。

### 親のstate directory

| もの | path | 読み手 | 今の挙動 | 規則 | 段階 | 証明 |
| - | - | - | - | - | - | - |
| runの行（run・task・attempt・placement・collectionなど、25.5の段6の表） | `coordinator.db` | `show`・`ls`・`retry`・回収・復旧 | runの刈り取りで消す（終わってから `retention_days` 日） | runが在る間。刈り取りで消す | 2 | 25.5 |
| event（runかattemptのもの） | `coordinator.db` の `event` | `show`・`logs`・配置の理由・GitHub status | runと一緒に消す（triggerは、行があって印の無いrunのeventの削除だけを禁じる。このrunのattemptを名指して別のrunを名指すものは、そのrunと一緒。行の無いattemptを名指すeventは書かない） | runと一緒 | 2 | 25.5 |
| event（どちらも持たないもの） | 同上 | 診断 | 書いてから `retention_days` 日で消す（1 passで1,000行） | `retention_days` 日 | 2 | 時刻 |
| `control_request` | `coordinator.db` | 同じ `request_id` の再送 | runと一緒に消す | runと一緒 | 1（列）、2 | 25.5 |
| 行の無いrunへのcancelの行 | `coordinator.db` の `run_cancel_intent`・`control_request`・`event`（そのrun ID） | 後から届くenqueue | 持ち主の行が `capturing` の取り込み（進行中か、持ち主が `given_up` を書かずに終わったもの）のrunについてだけ書く。enqueueが届かなければ、放棄の処理で消す（段階1より前は、どのrun IDへのcancelでも書き、消さなかった） | 持ち主の行が `capturing` の取り込みのrunについてだけ書く（取り込み1つにつき、予約1行、cancelの要求ごとに受領とeventが1行ずつ）。enqueueが届けばrunと一緒に、届かなければ放棄の処理で消す | 1 | 25.4の (b) |
| `pending_delete`・`result_delete_claim` | `coordinator.db` | 削除の再実行 | fileを消した後に消す | 同じ | 既存 | fileが無いこと |
| `ci_test_timing` | `coordinator.db` | Shardの分割 | 最後に書いてから14日で消す（1 passで1,000行） | 最後に書いてから14日 | 2 | 時刻 |
| 退役させたidentity | `coordinator.db` の `worker_identity_tombstone`・`identity_retired_notice` | 接続の拒否 | 消さない | 未決（25.16） | なし | なし |
| 外したメンバー | `coordinator.db` の `removed_member`。coordinator DBの無いhostでは `removed-members.json`（固定のfile、1個） | 接続の拒否 | 戻したときに消す。coordinator DBのあるhostに残っていた `removed-members.json` は、次の鍵の更新（`group rotate`）が表へ移して消す（読めなければ警告して残し、次の更新がまた試す） | 外した名前ごとに1行 | 既存、2（fileの移し替え） | 利用者の操作 |
| 結果 | `results/<attempt>/`（result・log・artifact・`tmp/*.part`） | `logs`・`outputs`・CIの後のJobのdownload | attemptごとに、終了と回収を閉じた時点の遅い方から `retention_days` 日。workflow artifactは、runが終わってから `retention_days` 日。spoolは1 GiB/task、artifactは1 GiB/upload | 同じ。刈り取りの段3が残りを消す | 既存（残りは2） | 排他pinと、回収が閉じていること |
| readerのpin | `reader-pins/<run>.lock` | `logs`・`outputs`・結果のGC | runの刈り取りの最後にunlinkする。作るのは、行があって印が無いときだけ。行の無いrunのpinは、起動時の走査が消す | runが在る間。作るのは、行があって印が無いときだけ | 2 | 25.1の (B)、25.5 |
| 入力（manifest・object） | `sources/manifests/`、`sources/objects/` | 配送、`retry`、workspaceの準備 | runが終わってから `retention_days` 日（終点は25.3の (a)）。取り込みは、内容が安定した後にだけここへ入れ、入れる前に索引へ予約を書く。提出されなかった取り込みが入れたものと、取り込みを止めた、どのrootも持たない内容の違うfileは、放棄の処理で消す（25.4の (b)） | 同じ | 既存 | 索引に参照が無いこと、排他lock |
| 保持の索引 | `sources/retention.db`（`root`・`object`・`member`・`graph`・`publication`・`run_expiry`・`source_delete_claim`・`vault_delete_claim`・`import_session`・`capture_owner`・`meta`） | 上の削除と配送 | runの行（runのrootとそのmember・graph、`run_expiry`、完了した `vault_delete_claim`、`import_session`、`capture_owner`）は刈り取りで、放棄された取り込みの行は放棄の処理で消す | runの行（runのrootとそのmember・graphを含む）は刈り取りで、放棄された取り込みの行は放棄の処理で消す。`capture_owner` は、進行中と放棄された取り込みの数だけ。`GuardedObjectStore::put_bytes` は製品の呼び出し元が無く、行を作らない。段階5bで消す | 1（`capture_owner` と放棄）、2（runの行）、5b（`put_bytes`） | 指す先が無いこと |
| runの取り込みの記録とlock | `sources/runs/<run>`、`<run>.lock` | `retry`、配送の検証 | 提出されたrunのものは刈り取りで、提出されなかった取り込みのものは放棄の処理で消す | runが在る間。提出されなければ、持ち主のprocessの終了か `given_up` の後、residentの次の出来事かpassまで | 1（放棄）、2（刈り取り） | 25.1の (A) |
| 平文のsecretの束 | `sources/secrets/<run>.json`、`.secrets.tmp-*` | その取り込み（Vaultへ移すまで） | 取り込みの中でVaultへ移して消す。途中で終わった取り込みのものは、放棄の処理で消す。一時fileは、保持driverが `sources/secrets.lock` を待たずに排他で取れたpassと、workerの起動（`sweep_store_copies`）で消す | Vaultへ移すまで。放棄された取り込みのものは放棄の処理で、一時fileは `sources/secrets.lock` を取れたpassで消す | 既存、1 | 25.4の (a)・(b) |
| bundle | `sources/bundles/<source>-<32hex>.bundle` | 進行中の配送（offsetからの再開） | sourceごとに4個まで。配送の成功時に消す。失敗・放棄で残ったものは、そのsourceを参照する最後のrunの刈り取りで消す | 加えて、そのsourceを参照するrunが無くなったときに消す | 2 | 索引に参照するrunが無いこと |
| 親のGit領域 | `sources/git/<key>.git`（`refs/local-ci/snapshots/<source>`・`index/<source>`・`heads/*` とGit object） | bundleの作成、次の取り込み | 消さない | refはsourceが在る間。objectはrefを消してから最長14日と24時間（25.5）。`heads/*` と、refを消す関数は未決（25.16）。取り込みの予約とscratch directoryの対象外で、途中で終わった取り込みが書いたobjectとrefも同じ扱いである | 2（関数があれば） | refが無いこと、Gitの猶予 |
| 取り込みの作業領域 | `sources/tmp/` | 書いたprocessだけ | 正常終了で消える。crashで残るものは、そのstoreの変更lockを排他で取れたpass（親は保持driver、workerは起動）が、名前かmarkerで所有を証明できたものだけ消す。取り込みが書く一時fileとcommitのcheckoutは、ここではなく、下のscratch directoryに書く | 25.4の (a) | 1 | 持ち主のprocessが居ないこと |
| 取り込みのscratch directory | `sources/staging/<ulid>/`（local-ciが作る `objects/`・`manifests/`・`secrets/`・`tmp/` と、commitの取り込みのcheckout `commit-<sha>-*/`） | その取り込みだけ | 取り込みが、戻る前に中身ごと消す（消せなければ取り込みは失敗する）。途中で終わった取り込みのものは、放棄の処理が、storeのfileより先に、中身ごと消す。計画の下書きのものは、下書きのdirectoryと一緒に消える | 取り込みの間だけ。提出されなければ、持ち主のprocessの終了か `given_up` の後、residentの次の出来事かpassまで | 1 | `capture_owner` の行が名指すこと、sourceの排他lock（取り込みは共有で持つ）、自分のuid・同じfilesystem・symlinkをたどらないこと（25.4の (b)） |
| 共有の固定lock | `sources/secrets.lock`、sourceの変更lock | 取り込み | 固定 | unlinkしない | 既存 | 該当なし |
| 計画の下書き | `ci-plan-*/` | 書いたcommandだけ | 正常終了で消える。crashで残るものは、計画の開始がmarkerで所有を証明できたものだけ消す | 25.4の (a) | 1 | 同上 |
| 親Vault | `secrets/<run>.json`、`parent-retention.json`、lock | 配送、`retry` | runが終わった時点で消す。放棄された取り込みのものは、放棄の処理で消す | 同じ | 既存、1（放棄） | 第23章のC、25.4の (b) |
| Vaultの一時file | `secrets/.<名前>.<ulid>.tmp` | 公開の続き | 名前（`.belt-tmp.…`）が所有を証明し、排他leaseの下で持ち主が居ないものを消す。名前を解釈できないものは数えるだけで残す（古い形の名前は、排他leaseの下で消す） | 25.4の (a) | 1 | 持ち主のprocessが居ないこと |
| FrozenPlan | `ci-plans/<digest>` | CI runの進行 | 名指す待ちのtaskが無くなって `retention_days` 日 | 同じ | 既存 | 名指すtaskが無いこと |
| in-placeのbinding | `in-place/<run>.json` | residentの再起動、`retry` | 提出を断ったときと、取り込みが放棄されたときに消す。受理されたrunのものは、刈り取りで消す | runが在る間 | 1（放棄）、2（刈り取り） | runの行が無いこと |
| 実測 | `stats/costs-v1.sqlite`（`costs`・`preparation_namespaces`・`preparation_receipts`・`cost_eviction`） | 配置の見積 | `costs` 4,096行、`preparation_namespaces` 4,096 | 同じ | 既存 | 順位 |
| 更新の試用 | `upgrade/`（`record.json`、退避したDBと設定） | rollback | 試用の間。同時に1つ | 同じ | 既存 | commitの記録 |
| 公開の直前のstage（鍵・binary・DB・入力・serviceの定義） | `.init-*` | `init` の続き | journalが名指すものは、`init` の回復が公開を済ませ、取り消しが消す。一時fileの走査は見ない。journalに書く前に終わった `init` の残りは、次の `init` が、回復の後に持ち主の居ないものを消す（config directory、binaryのdirectory。親のstateは、自分のjournalがそこのDBを公開済みのときだけ。DBのstageの横の `-wal`・`-shm`・`-journal`・`.init-lock` も同じ走査で消す）。serviceの定義のdirectoryの残りと、古い形の `.init-<ulid>` は残る（1回のkillにつき高々1個。serviceの定義は数KB）。親のstateのDBのstageは、どの `init` もそこのDBを公開しない間は残る（同じく1回のkillにつき高々1個と、その横の4つ） | 同じ | 1 | `init.json` に無いことと、持ち主 |
| 固定のfile | `coordinator.json`、`init.json`、`control.json`、`group-rotation.json`、`membership-undo.json`、`membership-restart`、`traffic-hold.json`、`upgrade-hold.json`、`request.sock`、`owner.token` | それぞれの操作と起動 | 各1個。上書きする | 同じ | 既存 | 各操作の規則 |
| 固定のlock | 25.1の表の最後の行のうち、親のもの | すべてのcommand | 各1個 | unlinkしない | 既存 | 該当なし |

### workerのstate directory

| もの | path | 読み手 | 今の挙動 | 規則 | 段階 | 証明 |
| - | - | - | - | - | - | - |
| attemptの行と子（`command_launch`・`lease`・`log_spool`・`blob_ref`）、attemptのevent | `worker.db` | 親の照会、復旧 | 規則のとおり（段階3で入れた） | ACKか `release_attempt` の受理で消す（成功した回収はACK、打ち切った回収は閉じた時点の解放）。どちらも来なければterminalから7日。段階3より前にACKされた行は、段階3の起動時に消す | 3 | 25.6 |
| 重複防止の記録 | `worker.db` | 遅れたsubmit、親の照会 | 規則のとおり（段階3で入れた） | 解放済みは、解放より前に読まれ得たsubmitの処理が終わった時点で消す（通常は数ミリ秒）。未解放は `release_attempt` まで。合計でメンバーごとに10,000件（約2 MB）を歯止めにする | 3 | 25.6の (c) |
| tombstone | `worker.db` の `tombstone` | 遅れたsubmit | 届かなかったものは、メンバーごとに10,000件、epochの前進で消す。受理したものは消さない | 受理したものは重複防止の記録に置き換える | 既存、3 | 24.4、25.6 |
| 忘れたメンバー | `worker.db` の `refused_member` | 接続の拒否 | 規則のとおり（段階3で入れた） | 命令ごとに1行。1,024行まで。消さない | 3 | 利用者の操作 |
| メンバーごとの記録 | `worker.db` の `member_epoch`・`group_member`・`retired_identity` | 接続の検査 | 規則のとおり（段階3で入れた） | `belt group forget` で消す | 3 | 利用者の操作 |
| workerのevent（attemptに属さないもの） | `worker.db` | 親の `watch_events`、診断 | 規則のとおり（段階3で入れた） | `retention_days` 日 | 3 | 時刻 |
| 捨てたattemptの一覧 | `worker.db` の `dropped_attempt` | agentの片付け | directoryを消した直後に消す | 同じ | 既存 | directoryが無いこと |
| attemptのdirectory | `attempts/<attempt>/`（`supervise.json`・`execution-binding.json`・`cleanup-pending`・`ci/` を含む） | supervisor、回収、別のscopeのsupervisorの照合 | 規則のとおり（段階3で入れた） | ACKか `release_attempt` の受理でその場（行のcommitの直後、lockより先）。無ければterminalから7日 | 3 | 25.6 |
| workspace | `workspaces/<attempt>` | attemptのcommand | terminalで消す。32個まで | 同じ | 既存 | terminalと、supervisorのlock |
| supervisorのlock | `supervisors/<attempt>.lock` | supervisor、agent（5か所）、別のscopeのsupervisorの照合 | 規則のとおり（段階3で入れた） | attemptの行とdirectoryの後に消す | 3 | 25.1の (A)、25.6 |
| secretの配置 | workspaceの中、`secret-index/<attempt>.json`、`.secrets.tmp-*`、`secrets/` | attemptのcommand | terminalで消す | 同じ | 既存 | terminal |
| 受け取ったsource | `sources/manifests/`、`sources/objects/` | `blob_verify`、workspaceの準備 | 消さない | 参照されていない（terminalでないattemptも、他の受け取ったsourceのgraphも名指さない）ものを、最後に使ってから14日で消す。store全体が20 GiBを超える間は、古い順に消す。共有のobjectは最後のmemberと一緒に消える。公開に失敗した受け取りのblobは、tokenの解放で消す | 5b | sourceの変更lockの排他と `worker.db` の確認（25.8の5bの (a)） |
| workerの保持の索引 | `sources/retention.db` | 受け取りの保護 | `import_session`・`root(kind='pending')`・`member` が、受け取ったblobごとに増える | 公開か放棄で消す | 5a | 25.8 |
| clone | `clones/<repo>` | bundleの展開、workspaceの準備 | 消さない。refは、受け取りを拒否したときだけ消す | directoryは消さない（数は `repository_id` の種類で頭打ち）。消したsourceのref（`snapshots/<S>`・`index/<S>`）は、付けたcloneごとの記録（`clone_ref`）が無くなったときに、同じ排他と取り込みlockの下で消し、Git objectは `git gc --prune=14.days.ago`（1つのcloneに1日1回まで）で回収する | 5b | Gitの14日の猶予（25.8の5bの (e)） |
| 受け取り途中 | `incoming/<source>/` | 送り手の再開 | 16個、合計64 GiB、1つ32 GiB。24時間進まなかったものは、16個に達したときだけ消す | 24時間進まなければ、tickで消す | 5a | 進んでいないこと |
| 取り込みの作業領域・socketの準備 | `sources/tmp/`、`.s*/` | 書いた処理だけ | 正常終了で消える。crashで残るものは、workerの起動（`agent.lock` の下）が、名前かmarkerで所有を証明できたものだけ消す | 25.4の (a) | 1 | `agent.lock` と持ち主 |
| setup template | `setup/templates/<key>/` と、最後に使った時刻のstamp `<key>.used` | setupの適用 | 20 GiB、14日、run scopeは1時間。stampは、templateの無いものを片付けのときに消す | 同じ | 既存 | copy中でないこと、pin |
| setup builder | `setup/builders/<id>/` | そのbuilder | 終了時と起動時に消す | 同じ | 既存 | 持ち主のprocessが居ないこと |
| toolchainのroot | `toolchains/<identity>/`（`install.lock`・`receipt.json` を含む） | attempt、setup、導入のhelper | 消さない | 14日、20 GiB。参照が無いとき。段階4より前のrootは、最初の起動で行を得る | 4 | 25.7の (b) |
| toolchainのtrash | `toolchains/.trash-*` | なし | 次の導入と起動時に消す | 同じ | 既存 | 22.2 |
| FrozenPlan | `ci-plans/<digest>` | CI task | 名指すattemptが無くなって `retention_days` 日 | 同じ | 既存 | 名指すattemptが無いこと |
| toolのlink | `tool-bin/<digest>/` | hookとcommand | 消さない | 指す先のbinaryと一緒 | 4 | 25.7の (a) |
| 更新の試用 | `upgrade/`、`<config>/bin/<digest>/worker.db.saved` | rollback | 試用の間 | 同じ | 既存 | commitの記録 |
| 一時workerのmarkerとlock | `ephemeral.json`（書く途中は `ephemeral.json.part`）、`ephemeral.lock` | 次の一時workerの起動 | 一時workerがstateを作るときに、lockを作って共有で持ってからmarkerを書き、lockを子のprocessへ継承させる。stateと一緒に消える（正常終了、または次の一時workerの起動。どちらもdirectoryのflockの下で、markerを最後に消す）。作成の途中でkillされてmarkerが無いまま残った `ephemeral.lock` と `ephemeral.json.part` も、次の一時workerの起動が、その2つしか無いことをlockの下で確かめて消す。stateの中にmountされた別のfilesystemには入らず、その親とmarkerを残す（外れた後の起動が消す） | stateと一緒 | 1 | 25.4の (d) |
| 外したメンバー（workerだけのhost） | `removed-members.json` | 接続の拒否 | 1個。coordinator DBが無いhostでは正 | 同じ | 既存 | 利用者の操作 |
| 固定のfile | `worker.json`、`boot_id`（起動の記録）、`control.json`、`upgrade-hold.json`、`traffic-hold.json`、`worker-service.json`、`worker-binding.json` とその更新の予定、`worker-socket.json`、`upgrade-trial*.json`、`agent.sock`、グループ鍵と次の鍵 | 起動、接続 | 各1個。上書きする。鍵の一時fileは、名前が所有を証明するので持ち主が居なければ次の起動で消す（古い名前は600秒） | 同じ | 既存 | 各操作の規則 |
| 受け取りのlock | `incoming/locks/<0〜63>.lock`、`incoming/locks/all.lock` | sourceの受け取り | 65個で頭打ち（source IDのhashで割り振る） | unlinkしない | 既存 | 該当なし |
| 固定のlock | 25.1の表の最後の行のうち、workerのもの | workerの処理 | 各1個 | unlinkしない | 既存 | 該当なし |

task-traffic-controllerの台帳と履歴の置き場は、製品では単独CLIと同じ `$XDG_STATE_HOME/task-traffic-controller` である（`src/config/runtime.rs` が設定から上書きする。`<state>/traffic` は既定値で、設定を通らない起動だけが使う）。

組み込んだライブラリへ渡すdirectoryと、その中の保持の責任は次のとおりである。beltは、ライブラリのstateの中を自分で消さない。

| ライブラリ | beltが渡すもの | 保持 |
| - | - | - |
| local-ci | attemptごとの `attempts/<attempt>/ci/{run,tools,lock,trash}` とworkspace。sourceのstoreの中の `manifests`・`objects`・`git` | attemptの分は、beltがattemptと一緒に消す。storeの中は、この章の規則でbeltが消す。Git領域のrefを消す関数は未決（25.16） |
| task-traffic-controller | 台帳と履歴の置き場（第16章） | そのライブラリの契約。0.11.0 から、保持の掃除（履歴・job・sidecar・一時file・lease）は、`exec::run_gated`（`belt exec`）と `managed::finish`（supervisorの各node。終了コード0のときだけ）の終わりに、6時間に1回、最大200 msで走る。`managed::register` は掃除しない。掃除は `Paths::config_file()` を読み、保持の数値は呼び出し側の設定とその大きい方を取る。beltのContextはcommandと同じ `(state, config)` の組で作るので、belt・単独CLI・掃除は同じ設定ファイルを見る。失敗は stderr の `task-traffic-controller: cleanup: ...` に出る（beltは抑えず、stderrの有無を成否に使わない）。beltは中を自分で消さない |
| fast-worktree | workerの `HOME` の下の、単独CLIと同じcache・pool・trash・kept ref（22.5） | そのライブラリの契約。beltはattemptごとに削除を呼ぶ。それ以外は、自動保持を持つ版を採用するときに確かめる |
| portool | workerの `HOME` の下の、単独CLIと同じ割当表（22.5） | そのライブラリの契約。beltはworkspaceの削除後に `portool::release_removed_in` を呼ぶ。それ以外は同上 |

### config directory

| もの | path | 読み手 | 今の挙動 | 規則 | 段階 | 証明 |
| - | - | - | - | - | - | - |
| beltのbinary | `bin/<digest>/belt`、link `bin/current`・`bin/resident` | serviceの入口、supervisor、builder、番人 | 版ごとに1つ増え、消さない | 名指すものが無い版を消す | 4 | 25.7の (a) |
| 切替の途中のlink | `bin/.resident.<pid>`、`bin/.current.<pid>` | 切替の処理 | 名前が所有を証明し、`upgrade`・`init` の開始とresident・workerの起動が、持ち主の居ないものを消す | 25.4の (a) の名前にする | 1 | 持ち主のprocessが居ないこと |
| 設定と鍵 | `profiles.toml`、`cluster.key`、`worker-cluster.key`、次の鍵、ユーザー設定 | 起動 | 各1個。鍵の一時fileは、名前が所有を証明するので持ち主が居なければ次の起動で消す（古い名前は600秒） | 同じ | 既存 | 各操作の規則 |
| 設定とjournalの一時file | config directoryの中の `.belt-tmp.*` | 書いたcommandだけ | 正常終了で消える。crashで残るものは、各commandの開始が名前で所有を証明できたものだけ消す | 25.4の (a) | 1 | 持ち主のprocessが居ないこと |
| serviceの定義 | LaunchAgentのplist、systemdのunit（OSの置き場）と、その一時file | OS | hostごとに1つ。一時fileはcrashで残るものを、serviceの定義を書くcommandの開始が、`.belt-tmp.` で始まる名前だけ見て消す | 定義は同じ。一時fileは25.4の (a) | 既存、1 | 利用者の操作、持ち主 |

### 利用者のrepository

beltは、利用者のrepositoryとworktreeに、残るものを何も書かない。取り込みはrepositoryを読むだけで、snapshotのcommitとrefは親のGit領域（`sources/git/`）に作る。`in-place` の実行が書くのは、利用者のcommandが書いたものだけである。`outputs` の展開先は利用者が指定したdirectoryで、beltは消さない。

### OSの一時directory

| もの | path | 読み手 | 今の挙動 | 規則 | 段階 | 証明 |
| - | - | - | - | - | - | - |
| `logs` のcursor | `$TMPDIR/belt-log-reader-*/` | その `logs` だけ | 正常終了で消える。killで残るものは、`logs` の開始がmarkerで所有を証明できたものだけ消す | 25.4の (a) | 1 | 持ち主のprocessが居ないこと |
| profileの検査 | `$TMPDIR/` の検査用directory | その検査だけ | 所有markerつき。次の検査の開始時に消す | 同じ | 既存 | 同上 |

### 開発用の道具

`scripts/s26` のDocker受入環境（`target/s26/` の中のbuild cacheと実行ごとのdirectory、Docker image）は製品のstateではない。`down` は実行ごとのcontainer・network・directoryを消す。build cacheとimageは次のbuildが読むので残す。`down --all` は、そのうえでportoolの割り当てを返し（`portool release`）、build cache（`linux-cache`・`native`）、作ったLinuxのbinary、`resources` と、今の `S26_ARCH` のworkerのimageを消す。pullしたbase imageと `harness.lock` は残す。

## 25.11 消す処理の担当と費用

| 担当 | 契機 | 行うこと | 1回の上限 | 段階 |
| - | - | - | - | - |
| 親の保持driver（resident） | 起動直後に1回、以後60秒に1回まで。重ならない | 結果・入力・Vaultの削除（既存）、放棄された取り込み、印のあるrunの続き、条件を満たしたrunの刈り取り、runに属さないevent、`ci_test_timing` | runは32件、参照・pathは32件、検証は64 MiB、行は1,000行/transaction | 既存、1、2 |
| 親の出来事の処理 | 相手の `ready`、提出の拒否、行の無いrunへのcancelを放棄を理由に断ったとき、回収の打ち切り、更新のcommit、刈り取りの最後、接続の確立 | bundle、放棄された取り込み（in-placeのbindingを含む）、古いbinary、`release_attempt`、照合 | 対象の件数だけ。照合は500件/page | 既存、1、3、4 |
| workerのagentのtick | 既存の監視の周期 | terminalの片付け、`ack_wait`、workerのevent、`incoming/` の24時間。受け取ったsourceの削除とcloneのrefの削除（60秒に1回まで）、cloneのgc（1日1回まで）、objectのbytesの埋め戻し | GCのbatch（既存）、eventは1,000行。sourceは32件・検証64 MiB/pass、排他は候補1つの間だけ。埋め戻しは1,000行/pass | 既存、3、5a、5b |
| workerの出来事の処理 | ACK・`release_attempt` の受理、submitの処理の終了、接続が済んだとき、builderとそのhelperの終了、attemptのterminal | attemptの行とdirectoryとlock、解放済みの重複防止の記録、toolchainの参照、rootの削除 | 対象の件数だけ | 3、4 |
| 起動時の走査（resident、worker） | processの起動ごとに1回。役割のlock（`coordinator.lock`・`agent.lock`）を持って行う | 下 | 下 | 1〜5a |
| commandの開始時 | `logs`、計画、検査、`init`・`group`・`workers`・`upgrade` | その場所の、持ち主の居ない一時file | そのdirectoryの `readdir` 1回 | 1 |

**起動時の走査の規則。** 走査するのは、件数が「terminalでないもの」か「固定の数」で頭打ちになるdirectoryだけである: `reader-pins/`、`sources/runs/`、`in-place/`、`sources/bundles/`、`sources/tmp/`、state直下の一時file、`supervisors/`、`attempts/`、`workspaces/`、`<config>/bin/`、`tool-bin/`。各段階の規則が持ち主と一緒にこれらを消すので、通常は残りが無く、各directoryの `readdir` 1回で終わる。各entryは、DBの主キーの検索1回で判定する。

- `reader-pins/` のentryは、親DBにそのrunの行が無ければ消す（排他で取ってからunlinkする）。pinを作るのはresidentだけで、走査はresidentが要求を受け付ける前に行うので、作成と競わない。刈り取りの段6と段8の間で落ちた分がこれに当たる。
- `sources/runs/`・`in-place/` のentryは、「runの行が無い」だけでは消さない（提出前の取り込みと区別できない）。消すのは、`capture_owner` の行を起点にした25.4の (b) と、刈り取りだけである。走査は件数を数え、行も持ち主も無いものを `doctor` に示す。
- `supervisors/` のentryは、worker DBにそのattemptの行が無く、`attempts/<attempt>/` も無く、lockを作らずに取れたときだけ消す（25.6）。`dropped_attempt` の行が残るattemptは、その続きとして消す。
- workerの起動時には、ほかに次を行う。段階3より前にACKされた行の削除（25.6の (c2)。試用中は行わない）、解放済みの重複防止の記録の削除、段階4より前のtoolchainのrootの行の作成（25.7。1回だけ）。親の起動時には、段階1のmigrationが、行の無いrunのcancelの行を消す（25.4の (b)）。
- `sources/objects/`・`results/`・`toolchains/` の中身のように大きいものは、起動時に走査しない（索引とclaimが受け皿である）。

**失敗の扱い。** 消せなかったもの（権限、差し替え、使用中）は残し、理由の種類と件数だけを記録して、次のpassでやり直す。消す処理の失敗で、受理済みの仕事やresidentを止めない。

## 25.12 書かないもの

次は、段階が入った後も含めて、書かないものである。括弧は、それを成り立たせる段階である。

- residentとworkerのlog file（既存。serviceの定義は出力先のfileを持たない）。
- 利用者のrepositoryの中のref、object、設定、lock（既存）。
- 回収が済んだ結果と、打ち切った回収の結果の、workerの側の写し（3）。
- 終わったattemptのworkspaceと、置いたsecret file（既存）。
- 刈り取ったrunの要約や履歴の行（2）。
- 配送に成功したbundle（既存）と、参照するrunの無いsourceのbundle（2）。
- 名指すものの無い版のbinaryとtoolのlink（4）。
- 版の検査で断ったbinaryの写し（既存）。
- 匿名で足りる一時file（既存。`outputs` の展開の作業用など。作った時点でunlinkする）。
- 持ち主より長く残る、持ち主ごとのlock file、marker、空のdirectory（1〜4。固定のlockは除く）。

## 25.13 schemaとwireの変更（段階ごと）

| 段階 | 親DB | worker DB | `sources/retention.db` | wire・CLI |
| - | - | - | - | - |
| 0 | `run.pruning_at_ms`。schemaの版を上げる | なし | なし | 断る理由 `run_pruned`。印の確認を再送の受領より先にする |
| 1 | `control_request.run_id` と索引（既存の行は `target_json` から埋める）。索引 `event(run_id, seq)`。`event_no_delete` を、行の無いrunのeventの削除だけを通す条件つきのtriggerに置き換える。行の無いrunの `run_cancel_intent`・`control_request`・`event` を消す | なし | `capture_owner(run_id, state, root, place, pid, start)`（版は上げない）。`root.kind` の値 `abandoned`。claimの印。取り込みの予約（`member` の行）とscratch directory（`sources/staging/<ulid>/`）は、表も列も足さない | 断る理由 `capture_abandoned`。行の無いrunへのcancelは、持ち主の行が `capturing` の取り込みのものだけ受理し、ほかは `NOT_FOUND`（挙動の変更）。`ephemeral.json`・`ephemeral.lock`。一時fileの名前の規則（場所はhash） |
| 2 | `retire_tombstone_no_delete` を外す。`event_no_delete` の条件を広げる（断るのは、行があって印の無いrunのeventと、そのようなrunのattemptだけを名指すeventの削除）。索引 `event(seq)`（部分）、`run(source_id)`、`run(secret_bundle_id)`、`run(run_id)`（印のあるrunの部分索引）、`placement(task_id)`、`matrix_group(run_id)`、`matrix_permit(group_id)`。`ci_test_timing.written_at_ms` と索引。schemaの版を上げる | なし | `import_session(run_id)` の索引 | なし |
| 3 | なし（submitと解放の排他は、親のprocessの中。打ち切りの時点の `release_attempt` と、照合は、既存の行から決まる） | 重複防止の記録の表、`refused_member`。`event_no_delete`・`tombstone_no_delete` を外す。submitの判定と挿入を1つのtransactionにする。段階3より前にACKされた行の削除（試用の後） | なし | 機能 `attempt_release_v1`、操作 `release_attempt`（controlの種類）・`list_unreleased`、`overloaded_reason.reason = "attempt_records"`、断る理由 `member_forgotten`、`belt group forget <member>` |
| 4 | なし | toolchainのrootの表と参照の表。既存のrootの行の作成（最初の起動） | なし | なし |
| 5a | なし | なし | なし（tokenの解放の処理を足す） | なし |
| 5b | なし | 列 `worker_attempt.source_id` と部分索引 `worker_attempt(source_id) WHERE state <> 'terminal'`（terminalでない行だけ `spec_json` から埋める）。schemaの版を上げる | 列 `object.bytes`（既存の行は各passが埋める。fileを消すときにobjectの行を消す）、`meta` の `store_bytes`・`gc:<clone>`・`ref_drop:<clone>:<source>`、列 `root.used_ms` と索引 `root(kind, used_ms)`、索引 `graph(source, root)`、表 `clone_ref(clone, source, root)`（migrationが `received` のrootから埋める）、rootの種類 `receipt`（5aのtoken）と `drop:` のroot（`abandoned`） | code `INPUT_MISSING`（`source_id`、公開の時は足りないblobの一覧）、機能 `input_missing_v1`、`OVERLOADED` の理由 `source_store`。`GuardedObjectStore::put_bytes` を消す（公開のAPIではない） |

## 25.14 他の章との関係

他の章の文は、今の実装の挙動を書いている。段階が入ると、次の文が変わる。段階を実装する変更が、その文を書き換える。

| 章 | 今の文 | 変える段階 | 変わった後 |
| - | - | - | - |
| 第6章 | 受理されたrunのprivate bindingは、親stateの削除まで保持する（放棄された取り込みのbindingは、段階1で消すようにしてある） | 2（入れてある） | runの刈り取りで消す |
| 第6章 | 内容をprivateなobject storeへ取り込み、安定したらmanifestをatomicに公開する（やり直した回のobjectは、storeに残った） | 1（入れてある） | 内容は取り込みごとのscratch directoryへ書き、安定した後にだけ、索引へ予約を書いてからstoreへ入れる（25.4の (b)） |
| 第11章 | retire・重複排除の最小記録はclusterの寿命中保持する | 2（親。入れてある）、3（worker。入れてある） | 親の記録はrunと一緒に、workerの記録は25.6の (c) で消す |
| 第11章 | `list_attempts` は500件を超えると `INVALID_INPUT` | 変えない | 照合には `list_unreleased` を使う |
| 第13章 | 入力、ログ、結果の既定保持はterminal後7日。回収の判定の記録は消さない | 2（入れてある）、3 | 回収の記録はrunと一緒に消す。workerの写しは解放の受理で消す |
| 第13章 | 打ち切った回収ではACKを送らず、開き直せる | 3（入れてある） | ACKは送らない。打ち切りの時点で `release_attempt` を送り、workerはその場で写しを消す。開き直しはできなくなる（開き直す経路は製品に無い。足す変更が、写しを残す期間を定める。25.6） |
| 第13章 | resultなしで確定したattemptに後からresultが保存された場合の検出（`ack_collected` の `CONFLICT`、digestを照合した読み取りの後の保存の拒否） | 変えない | 成功した回収のACKは変わらない。`release_attempt` はdigestを運ばないが、resultを持たないattemptへは、supervisorのlockを取れたときだけ受理する |
| （他の章に記述は無い。段階1より前の実装は、行の無いrun IDへのcancelを受理して記録した） | なし | 1（入れてある） | 持ち主の行が `capturing` の取り込みのrunへのcancelだけを受理し、ほかは `NOT_FOUND`（持ち主の行を読めなかったときは、送り直しを求める答え。25.4の (b)） |
| 第15章 | 退役したidentityはtombstoneを永続保持する | 未決 | 25.16 |
| 第15章 | バイナリはversion別directoryへ配置する | 4 | 名指すものの無い版を消す |
| 第15章 | 残ったstateは、次の一時workerが、使われていないことを証明できたときだけ消して起動する（「起動し直し」。段階1で書き足した。それより前は記述が無く、実装はstateが残っていると起動を断っていた） | 1（実装済み） | 25.4の (d) |
| 第23章 | reader-pinsのinodeはparent stateとともに保持し、unlinkしない | 2（入れてある） | runの刈り取りの最後にunlinkする |
| 第23章 | sourceのrun fence、完了したclaim、`vault_delete_claim`、root・graph・objectの索引は恒久に残す | 2（入れてある） | runの刈り取りで消す。runの行が無いことが、fenceの代わりに断る（fenceを確かめる経路は、runの行があるときにだけ到達する） |
| 第23章 | Git・bundleの片付けは後続の作業 | 2（bundleは入れてある。Gitのrefは25.16） | 25.5の段4 |
| 第23章 | Future expiry of received roots also requires per-import pending ownership | 5a、5b | 25.8が持ち主を定め（5a）、受け取ったrootの削除を定める（5b） |
| 第13章 | 受理を後の照合へ回すのは、sourceがまだworkerのstoreに無い場合だけ。artifactを宣言しないpayloadでは、受理の時点でsource manifestを読まない | 5b | 新しいattemptの受理は必ずsourceの有無を確かめ、無ければ `INPUT_MISSING` で断る（`input_missing_v1` を交渉しない親には今のまま） |
| 第11章 | `blob_verify` は何も変更しない | 変えない | 公開の時に、`present` と答えたblobが消えていれば、拒否せず `INPUT_MISSING` で答え、親が `blob_verify` からやり直す |
| 第23章 | Vaultの、所有を証明できない一時fileは残す | 1 | 実装済み: 名前（`.belt-tmp.…`）で所有を証明し、排他leaseの下で消す。解釈できない名前は残す |
| 24.4 | 受理したattemptのtombstoneは消さない | 3 | 重複防止の記録に置き換える |
| 24.8 | 試用中のworkerとcoordinatorが行わないことの一覧 | 3 | `release_attempt` を足す |

第23章の恒久のfenceを外してよい理由: run IDはその場で作る新しい値で使い回さず、fenceを確かめる場所はrunの行があるときにだけ到達し、takeoverは既存のcoordinatorのstateを拒む。fenceを消す段5から、runの行を消す段6までは、印がretryを断る。

## 25.15 採らなかった案

- runの行を、結果より長く（グループの寿命の間）残す。読み手が無い。
- journalを追記専用のまま残し、古い分を別fileへ退避する。読み手が無いものを移すだけである。
- 刈り取りの印から最後まで、排他pinを持ち続ける（25.3）。
- `control_request` を、答えを返した時点で消す（25.5）。
- 親のGit領域とcloneで `git gc --prune=now` を使う（25.5、25.8の5b）。
- 受け取ったsourceの参照を `retention.db` の行で持つ（受理の2つのDBが1つのtransactionにならず、持ち主の居ない参照を拾う走査が要る）。sourceの変更lockと `worker.db` の直接の確認で足りる。
- 親の `ready` に期限を持たせる。新しい周期の処理が要り、期限の内でもworkerは消し得る。型つきの断りで、消えたことをその場で親へ伝える。
- 受け取ったsourceの量を、sourceごとのbytesで数える。objectは複数のsourceが共有し、sourceごとの合計は意味を持たない。storeの合計を上限で抑える。
- cloneのdirectoryを、workspaceが無いときに消す。cloneの中のlockについて25.1の (A) も (B) も示せない（25.8の5bの (e)）。
- 回収を閉じたら、閉じ方に依らず `ack_collected` を送る（25.6）。
- 打ち切った回収のworkerの写しを、開き直しに備えて `ack_wait` の間、またはrunの刈り取りまで残す。開き直す経路が製品に無く、読み手の無いものを最長7日残すことになる。
- 解放済みの重複防止の記録を、接続が閉じるまで残す。長く開いた接続で上限まで溜まり、断りが日常になる。接続が閉じても、取り出し済みのsubmitの処理は終わっていないことがあり、証明としても足りない。
- 行の無いrunへのcancelを、どのrun IDについても受理して記録する。enqueueの届かないrun IDの行を消すものが無い。
- 放棄された取り込みを、CLIが自分で全部消す。CLIは、sourceの削除の所有と親DBを持たない。
- `supervisors/<attempt>.lock` を、task-traffic-controllerの台帳にentryが無いことを根拠にunlinkする。照合は台帳を読まず、`attempts/*/supervise.json` を読む。
- `ack_collected` に「放棄」を表す任意の項目を足す。必須の `generation` と `result_digest` を、刈り取った後の親は持っていない。別の操作にする方が、digestの照合を弱めずに済む。
- 重複防止の記録を時間だけで、またはepochの前進だけで消す。前者は証明にならず、後者は親が長く再起動しない間に際限なく増える。
- 重複防止の記録に上限を置かない。回収も解放もしない親の未解放の記録が増え続ける。
- `workers forget <member>` に、workerの側でメンバーを忘れる意味を足す（25.6）。
- 忘れたメンバーの断りを、鍵の更新で消す（25.6）。
- 一時workerのhostを、hostnameとPID namespaceの番号で識別する（25.4）。
- 一時workerの残ったstateを、人が消すまで起動を断る。所有markerと生存のlockで、自分で片付けられる。
- 提出前の取り込みを、`sources/runs/<run>.lock` のflockで保護する。enqueueの側が同じrunのguardを取るので、取り込むprocessがlockを持ち続ける形にできない。持ち主の行にする。
- toolchainのrootの削除を、`install.lock` とinodeの確認だけで守る。既存のrootをbindする側はlockを取らないので、守れない。
- rootを消すために、その上のsetup templateを同じpassで消す。templateは自分のpinとcopy中の保護を持つ。
- 持ち主ごとのlockを「小さい」として残す。件数が実行の数だけ増える。
- 誰もが取るlockを、inodeの確認を足してunlinkする。PATHに残る古いCLI、更新をまたぐbuilder、番人が、確認なしで開く。

## 25.16 未決

次は、消す時点をこの版では定めていない。閉じるのに要るものを書く。

- **常駐workerの退役させたidentityの記録**（親DBの `worker_identity_tombstone`・`identity_retired_notice`）。件数は退役の回数（利用者の `workers forget` と、同じ名前の新しいidentityの接続）で、実行の数では増えない。閉じるには、同じidentityが戻らないことを何が証明するかを決める必要がある。外したメンバーは `group send-key` で戻れ、戻るときにworkerのidentityを作り直す（24.2）が、作り直す前のidentityのprocessが残っていないことを、親の側から確かめる手段が無い。一時workerのidentityは「起動時にだけ鍵を受け取るので、鍵の更新の後は接続できない」と言えるが、親の記録に、そのidentityが一時workerのものだったという項目が無い。項目を足すか、workerの側の証明（identityを作り直した記録を親へ届ける）を足すかを決める。
- **親のGit領域とcloneのref**（`refs/local-ci/snapshots/*`・`refs/local-ci/index/*`・`refs/local-ci/heads/*`）。beltは、local-ciのGit領域の中を自分で書き換えない。取り込みがstoreへ入れるobjectとmanifestは、scratch directoryと予約で数えるようになった（25.4の (b)）が、Git領域のobjectとrefは、今もlocal-ciが取り込みの途中で直接書く。途中で終わった取り込み・入力が動いてやり直した取り込みが書いた分も、この項目に含まれる。閉じるには、local-ciのライブラリに次の関数が要る: (1) store（またはclone）とsource IDの集合を受け取り、「残すsource」以外のsnapshotとindexのrefを消す。(2) `heads/*`（履歴の取り込みに使う）について、残すsourceのどれからも要らなくなったものを消す。どれが要るかはlocal-ciだけが知っている。(3) 消したrefの数を返し、進行中の取り込みと競合しない（local-ci自身のlockの下で行う）。beltは、刈り取りの段4から、残すsourceの集合を渡してこの関数を呼ぶ。この関数が無い間、refと、それが指すGit objectは残る。採用している版（local-ci 0.18.5）と0.19.0には、この関数は無い: `remove_imported_refs` はあるが、cloneに `import_objects` が付けたrefを消す関数で、storeのGit領域を対象にせず、「残すsource」の集合も `heads/*` も扱わず、進行中の取り込みと競合しないためのlockも取らない。したがって段階2は、refの削除と `git gc` を入れていない（25.5の受け入れ試験の (xi) は、`git gc` を呼ぶコードが無いので該当しない）。 workerのcloneは、この項目から外れる: workerの受け取りが付けるrefは消すsourceのsnapshotとindexの2つだけで（`heads/*` は付けない）、段階5bがsourceごとに `remove_imported_refs` を、取り込みlockの下で呼ぶ（25.8の5bの (e)）。
- **受け取りを拒否したときの、空のcloneの削除**（25.9）。段階5bのほかの部分は25.8で定めた。
- **retireを確認したattemptのworkerの写し。** 親がretireを確認したattemptは、親がその結果を読まない。workerが受理していたものは、今は `ack_wait`（7日）か、runの刈り取りの `release_attempt` まで、workerに写しが残る。確認の時点で `release_attempt` を送れば、その場で消せる。閉じるには、(1) 親が、retireを確認した後に、そのattemptのsubmitを送り直さないこと（25.6の (c) の証明は「terminalの記録の後は送らない」に依っており、retire済みのattemptは親の側でterminalにならない）、(2) workerの側で、retireを確認したattemptがterminalになっていること（`release_attempt` はterminalのattemptだけ受理する）、を実装で確かめる必要がある。
- **導入のhelperが残っていないことの確認**（25.7の (b)）。規則は書いたが、helperがどのprocess groupで起動されるか、今のbuilderの片付けがその終了を確かめているかは確認していない。段階4の実装の最初に確かめる。
- **組み込んだライブラリの自動保持**（task-traffic-controller、fast-worktree、portool）。それぞれの版の採用のときに、beltが呼ぶ関数を25.10の表に書く。
- **予約をしない以前の版が残した、名指すものの無いobjectとmanifest。** local-ci 0.19.0を採用する前の版は、objectとmanifestを、memberとして登録する前にstoreへ直接書いた。その版の取り込みが途中で終わるか、入力が動いてやり直すと、どのrootのmemberでもないfileが残った。この版からは増えない（25.4の (b)）。既に在る分は、同じ内容を後の取り込みが予約したときにだけ索引に入り、ほかは残る。閉じるには、索引に行の無いfileを `sources/objects/` の全走査で探す処理と、その間に進行中の取り込みが無いこと（sourceの排他lock）の下で消す規則が要る。
- **他のrootが持つ、内容の違うobjectとmanifest。** 取り込みを止めた宛先のfileを、他のrootがmemberに持つときは、内容が違っても消さない（25.4の (b)）。放棄の処理は数える（`corrupt_held`）だけで、そのfileを持つrootの内容が欠けたまま残る。閉じるには、そのrootのrunを失敗として扱うか、fileを正しい内容で置き換える規則が要る。
