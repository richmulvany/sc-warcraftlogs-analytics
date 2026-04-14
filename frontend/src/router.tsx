import { createBrowserRouter } from 'react-router-dom'
import { Overview }      from './pages/Overview'
import { Players }       from './pages/Players'
import { PlayerDetail }  from './pages/PlayerDetail'
import { Bosses }        from './pages/Bosses'
import { WipeAnalysis }  from './pages/WipeAnalysis'
import { Raids }         from './pages/Raids'
import { Attendance }    from './pages/Attendance'
import { Roster }        from './pages/Roster'
// Legacy pages kept but accessible via guild nav
import { Performance }   from './pages/Performance'

export const router = createBrowserRouter([
  { path: '/',                      element: <Overview />     },
  { path: '/players',               element: <Players />      },
  { path: '/players/:playerName',   element: <PlayerDetail /> },
  { path: '/bosses',                element: <Bosses />       },
  { path: '/wipe-analysis',         element: <WipeAnalysis /> },
  { path: '/raids',                 element: <Raids />        },
  { path: '/attendance',            element: <Attendance />   },
  { path: '/roster',                element: <Roster />       },
  { path: '/performance',           element: <Performance />  },
])
