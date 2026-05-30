import { Component, inject, computed } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { CommonModule } from '@angular/common';
import { AuthService } from '../../../core/services/auth.service';
import { SidebarService } from '../../../core/services/sidebar.service';

@Component({
    selector: 'app-sidebar',
    standalone: true,
    imports: [CommonModule, RouterLink, RouterLinkActive],
    templateUrl: './sidebar.component.html',
    styleUrl: './sidebar.component.css'
})
export class SidebarComponent {
    private auth = inject(AuthService);
    svc          = inject(SidebarService);
    currentUser  = computed(() => this.auth.currentUser());

    navItems = [
        { path: '/dashboard', icon: 'bar_chart',            label: 'Thị trường' },
        { path: '/stocks',    icon: 'format_list_bulleted',  label: 'Cổ phiếu'  },
        { path: '/watchlist', icon: 'bookmark',              label: 'Watchlist'  }
    ];

    logout() { this.auth.logout(); }
}
