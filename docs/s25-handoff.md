# S25・top の引き継ぎ（作業中断時点）

このファイルは引き継ぎ専用ブランチに置いた。実装ブランチは次の2本。両方とも push 済み。

1. RTR S25 は `integration/s25-acceptance` の `2bdacdf48842169b49b4c0a5f813f8aeef2baf7c`。[PR #82](https://github.com/t09tanaka/remote-task-runner/pull/82) は未マージ。[対象SHAのActions](https://github.com/t09tanaka/remote-task-runner/actions/runs/44295111937) は中断時点で実行中。まずこのrunを照会する。ローカルでは実行可能な8ジョブが成功し、nextest 2501件成功・11件skip、純粋モジュール59件の行カバレッジ100%、変異の生存・timeoutとも0。macOSのlocal-ciはLinuxとinstall actionsを再現できず総合incomplete。旧SHAのLinux失敗3件はCPU数依存のテスト、retentionテストの同期、同時blob受信のSQLite書込み競合で、このSHAに修正を含む。
2. RTR top は `feature/s25-live-top` の `e2101c026c45c808575fabd4ca908bad2db027a7`。上記S25修正、公開版local-ci 0.18.3固定、portoolタグ、実端末受入、公開前に名前を再検討する旨の計画を含む。PRは未作成。ローカルnextestは2506件成功・11件skip、全CIは中断したので成功扱いにしない。残りは同SHAのlocal-ci結果を照合して完走、PRを作成しリモートCIを確認。S25がmainへ入ってからtopの差分とmergeableを確認する。統合はどちらも gh pr merge --merge を使用する。
3. 各マージのSHAを記録し、実機不要のS25受入を最終mainでも確認する。受入対応表は docs/plan.md、A01〜A49のうちA37と明記された実機部分はS26。portool sync/check後、task-traffic-controller exec -- portool exec -- env RTR_REQUIRE_FAST_WORKTREE=1 RTR_REQUIRE_TTC_CLI=1 cargo nextest run --locked --workspace --all-targets を使用。最終SHAでA46切断・明示forget・旧identity拒否、A48のdirty snapshotとポート解放、top_refreshesの実PTY・リサイズ・終了後の端末復元を確認する。確認後、対応表の統合検証待ちを実測結果で更新する。リリース、改名、TTCの廃止は今回の範囲外。
4. TTC top の [portool表示](https://github.com/t09tanaka/task-traffic-controller/pull/44) と [日本語の幅修正](https://github.com/t09tanaka/task-traffic-controller/pull/45) はマージ済み。双方ともGitHub Actions全5件成功。前者はマージ後に実バイナリ＋実portoolで表示と端末復元を検証、後者はマージ後 f8dfa00 で描画38試験成功。TTC側の再実装は不要。

ローカルのCIログは ~/Library/Caches/local-ci/runs/remote-task-runner-30f48edc/ 内のSHA末尾で識別できる。worktree専用 .env.portool はコミットしない。
