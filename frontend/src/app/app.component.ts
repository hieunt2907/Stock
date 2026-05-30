import { Component, inject, signal } from '@angular/core';
import { RouterOutlet, Router, NavigationEnd } from '@angular/router';
import { CommonModule } from '@angular/common';
import { filter } from 'rxjs';
import { SidebarComponent } from './shared/sidebar/sidebar.component';
import { SidebarService } from '../core/services/sidebar.service';

@Component({
    selector: 'app-root',
    standalone: true,
    imports: [RouterOutlet, CommonModule, SidebarComponent],
    templateUrl: './app.component.html',
    styleUrl: './app.component.css'
})
export class AppComponent {
    private router  = inject(Router);
    sidebarSvc      = inject(SidebarService);
    showSidebar     = signal(false);

    constructor() {
        this.router.events.pipe(
            filter(e => e instanceof NavigationEnd)
        ).subscribe((e: any) => {
            const url: string = e.urlAfterRedirects ?? e.url;
            const isAuth = url === '/login' || url === '/register' || url.startsWith('/login') || url.startsWith('/register');
            this.showSidebar.set(!isAuth);
        });
    }
}
