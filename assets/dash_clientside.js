window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        toggle_theme: function (n1, n2, current_theme) {
            console.log(">>> Toggle Action | N1:", n1, "N2 (Mobile Sidebar):", n2, "Store:", current_theme);
            if (!n1 && !n2) return window.dash_clientside.no_update;

            const new_theme = (current_theme === 'dark') ? 'light' : 'dark';
            console.log(">>> Switching to:", new_theme);

            localStorage.setItem('persistent-theme', new_theme);

            const apply = (theme) => {
                const body = document.body;
                if (theme === 'light') body.classList.add('light-theme');
                else body.classList.remove('light-theme');

                const icon = document.querySelector('#theme-toggle i');
                const iconM = document.querySelector('#theme-toggle-mobile i');
                const iconType = (theme === 'light') ? 'fa-moon' : 'fa-sun';
                if (icon) icon.className = 'fas ' + iconType;
                if (iconM) iconM.className = 'fas ' + iconType;
            };

            apply(new_theme);
            return new_theme;
        },

        init_theme: function (theme) {
            const localStored = localStorage.getItem('persistent-theme');
            const activeTheme = localStored || theme || 'dark';
            console.log(">>> Init Sync | Store:", theme, "| Local:", localStored, "| Selected:", activeTheme);

            const apply = () => {
                const body = document.body;
                const icon = document.querySelector('#theme-toggle i');
                const iconM = document.querySelector('#theme-toggle-mobile i');
                const iconType = (activeTheme === 'light') ? 'fa-moon' : 'fa-sun';

                if (activeTheme === 'light') {
                    body.classList.add('light-theme');
                } else {
                    body.classList.remove('light-theme');
                }

                if (icon) icon.className = 'fas ' + iconType;
                if (iconM) iconM.className = 'fas ' + iconType;
            };

            apply();
            setTimeout(apply, 100);
            setTimeout(apply, 500);
            setTimeout(apply, 1500);

            return window.dash_clientside.no_update;
        },

        toggle_mobile_menu: function (n_clicks) {
            if (!n_clicks) return window.dash_clientside.no_update;
            const sidebar = document.querySelector('.sidebar');
            if (sidebar) sidebar.classList.toggle('show');
            return window.dash_clientside.no_update;
        },

        null_handler: function (url_list) {
            return null;
        }
    }
});
