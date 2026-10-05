# Road to Campus by Alutta

Files:
- index.html: the game
- api/game.js: online features (accounts, leaderboard, wallet, shop, transfers, loans)
- vercel.json: hosting settings

Setup on Vercel:
1. Put these files in a GitHub repo, keeping game.js inside a folder named api.
2. In Vercel: Add New > Project > import the repo > Deploy.
3. In the project: Storage > Create Database > Upstash for Redis (free plan) > connect it to this project.
4. Redeploy once (Deployments > ... > Redeploy) so the function picks up the database keys.

Challenges: one player creates a 6-letter code from Play with friends and shares it. Friends join on their own devices, play the same road, and see a shared standings table. Only each player's first try counts. Codes last 30 days.

Shop prices live in the CATALOG list at the top of api/game.js.
Game points have no cash value. 1 point is shown as 1,000 naira for fun only.
