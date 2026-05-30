import { Routes } from '@angular/router';

export const routes: Routes = [
    { path: '', redirectTo: 'dashboard', pathMatch: 'full' },
    { path: 'login',    loadComponent: () => import('./features/auth/login/login.component').then(m => m.LoginComponent) },
    { path: 'register', loadComponent: () => import('./features/auth/register/register.component').then(m => m.RegisterComponent) },
    { path: 'dashboard', loadComponent: () => import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent) },
    { path: 'stocks',   loadComponent: () => import('./features/stocks/stock-list.component').then(m => m.StockListComponent) },
    { path: 'stocks/:symbol', loadComponent: () => import('./features/stocks/detail/stock-detail.component').then(m => m.StockDetailComponent) },
    { path: '**', redirectTo: 'dashboard' },
];
