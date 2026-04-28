import { lazy, Suspense } from 'react'
import { Navigate, createBrowserRouter } from 'react-router-dom'
import { Overview }      from './pages/Overview'
import { Players }       from './pages/Players'
import { Bosses }        from './pages/Bosses'
import { BossDetail }    from './pages/BossDetail'
import { Raids }         from './pages/Raids'
import { RaidDetail }    from './pages/RaidDetail'
import { Attendance }    from './pages/Attendance'
import { Roster }        from './pages/Roster'
import { Preparation }   from './pages/Preparation'
// Legacy pages kept but accessible via guild nav
import { Performance }   from './pages/Performance'

const PlayerDetail  = lazy(() => import('./pages/PlayerDetail').then(m => ({ default: m.PlayerDetail })))
const MythicPlus    = lazy(() => import('./pages/MythicPlus').then(m => ({ default: m.MythicPlus })))
const WipeAnalysis  = lazy(() => import('./features/wipe-analysis'))

function Lazy({ children }: { children: React.ReactNode }) {
  return <Suspense fallback={<div className="min-h-screen bg-ctp-base" />}>{children}</Suspense>
}

export const router = createBrowserRouter([
  { path: '/',                       element: <Overview />     },
  { path: '/players',                element: <Players />      },
  { path: '/players/:playerName',    element: <Lazy><PlayerDetail /></Lazy> },
  { path: '/bosses',                 element: <Bosses />       },
  { path: '/bosses/:encounterId/:difficulty', element: <BossDetail /> },
  { path: '/boss-wipes',             element: <Navigate to="/wipe-analysis" replace /> },
  { path: '/wipe-analysis',          element: <Lazy><WipeAnalysis /></Lazy> },
  { path: '/raids',                  element: <Raids />        },
  { path: '/raids/:reportCode',      element: <RaidDetail />   },
  { path: '/attendance',             element: <Attendance />   },
  { path: '/roster',                 element: <Roster />       },
  { path: '/preparation',            element: <Preparation />  },
  { path: '/mythic-plus',            element: <Lazy><MythicPlus /></Lazy> },
  { path: '/performance',            element: <Performance />  },
])
