import { createBrowserRouter } from 'react-router-dom'
import { Overview }      from './pages/Overview'
import { Players }       from './pages/Players'
import { PlayerDetail }  from './pages/PlayerDetail'
import { Bosses }        from './pages/Bosses'
import { BossWipes }     from './pages/BossWipes'
import { WipeAnalysis }  from './pages/WipeAnalysis'
import { Raids }         from './pages/Raids'
import { RaidDetail }    from './pages/RaidDetail'
import { Attendance }    from './pages/Attendance'
import { Roster }        from './pages/Roster'
// Legacy pages kept but accessible via guild nav
import { Performance }   from './pages/Performance'

export const router = createBrowserRouter([
  { path: '/',                       element: <Overview />     },
  { path: '/players',                element: <Players />      },
  { path: '/players/:playerName',    element: <PlayerDetail /> },
  { path: '/bosses',                 element: <Bosses />       },
  { path: '/boss-wipes',             element: <BossWipes />    },
  { path: '/wipe-analysis',          element: <WipeAnalysis /> },
  { path: '/raids',                  element: <Raids />        },
  { path: '/raids/:reportCode',      element: <RaidDetail />   },
  { path: '/attendance',             element: <Attendance />   },
  { path: '/roster',                 element: <Roster />       },
  { path: '/performance',            element: <Performance />  },
])
