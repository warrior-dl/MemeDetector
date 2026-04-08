import { lazy, Suspense, type ReactNode } from "react";
import { Skeleton } from "antd";
import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./AppShell";

const DashboardPage = lazy(() =>
  import("../pages/DashboardPage").then((module) => ({ default: module.DashboardPage })),
);
const ScoutPage = lazy(() =>
  import("../pages/ScoutPage").then((module) => ({ default: module.ScoutPage })),
);
const CandidatesPage = lazy(() =>
  import("../pages/CandidatesPage").then((module) => ({ default: module.CandidatesPage })),
);
const MinerPage = lazy(() =>
  import("../pages/MinerPage").then((module) => ({ default: module.MinerPage })),
);
const LibraryPage = lazy(() =>
  import("../pages/LibraryPage").then((module) => ({ default: module.LibraryPage })),
);
const PipelinePage = lazy(() =>
  import("../pages/PipelinePage").then((module) => ({ default: module.PipelinePage })),
);

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<PageLoader><DashboardPage /></PageLoader>} />
        <Route path="/scout" element={<PageLoader><ScoutPage /></PageLoader>} />
        <Route path="/miner" element={<PageLoader><MinerPage /></PageLoader>} />
        <Route path="/candidates" element={<PageLoader><CandidatesPage /></PageLoader>} />
        <Route path="/library" element={<PageLoader><LibraryPage /></PageLoader>} />
        <Route path="/pipeline" element={<PageLoader><PipelinePage /></PageLoader>} />
      </Route>
    </Routes>
  );
}

function PageLoader({ children }: { children: ReactNode }) {
  return (
    <Suspense fallback={<Skeleton active paragraph={{ rows: 10 }} />}>
      {children}
    </Suspense>
  );
}
