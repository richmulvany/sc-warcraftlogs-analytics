import { Navigate, createBrowserRouter } from 'react-router-dom'
import { Overview }      from './pages/Overview'
import { Players }       from './pages/Players'
import { PlayerDetail }  from './pages/PlayerDetail'
import { Bosses }        from './pages/Bosses'
import { BossDetail }    from './pages/BossDetail'
import { WipeAnalysis }  from './pages/WipeAnalysis'
import { Raids }         from './pages/Raids'
import { RaidDetail }    from './pages/RaidDetail'
import { Attendance }    from './pages/Attendance'
import { Roster }        from './pages/Roster'
import { Preparation }   from './pages/Preparation'
// Legacy pages kept but accessible via guild nav
import { Performance }   from './pages/Performance'

export const router = createBrowserRouter([
  { path: '/',                       element: <Overview />     },
  { path: '/players',                element: <Players />      },
  { path: '/players/:playerName',    element: <PlayerDetail /> },
  { path: '/bosses',                 element: <Bosses />       },
  { path: '/bosses/:encounterId/:difficulty', element: <BossDetail /> },
  { path: '/boss-wipes',             element: <Navigate to="/wipe-analysis" replace /> },
  { path: '/wipe-analysis',          element: <WipeAnalysis /> },
  { path: '/raids',                  element: <Raids />        },
  { path: '/raids/:reportCode',      element: <RaidDetail />   },
  { path: '/attendance',             element: <Attendance />   },
  { path: '/roster',                 element: <Roster />       },
  { path: '/preparation',            element: <Preparation />  },
  { path: '/performance',            element: <Performance />  },
])
