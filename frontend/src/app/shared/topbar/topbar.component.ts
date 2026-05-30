import { Component, inject, computed } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { AuthService } from '../../../core/services/auth.service';

@Component({
    selector: 'app-topbar',
    standalone: true,
    imports: [RouterLink, RouterLinkActive],
    templateUrl: './topbar.component.html',
    styleUrl: './topbar.component.css'
})
export class TopbarComponent {
    private auth = inject(AuthService);
    currentUser  = computed(() => this.auth.currentUser());

    logout() {
        this.auth.logout();
    }
}
