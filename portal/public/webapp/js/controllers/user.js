/*
 * Kweecker iPad app
 * Author: Neat projects <ties@expertees.nl>
 *
 * User controller
 */
app.controller('UserCtrl', function($scope, $rootScope, $window, $location, api, $routeParams) 
{

	// set the title
	$rootScope.title = $rootScope.lang.login_title;

	$scope.init = function()
	{
		// hide splash
		$rootScope.showSplash = false;
		
		// check if we're authenticated
		if(api.getApiToken() != null && $rootScope.pageSlug != 'logout')
		{
			api.authenticate(); // get user specs
			$location.path('/dashboard');
		}
	};


	$scope.formStatus = '';
	$scope.message	  = null;
	$scope.error   	  = null;
	$scope.fields     = {};

	$scope.logout = function()
	{
		api.reset();
		$location.path('/login');
	}


	$scope.resetErrors = function()
	{
		$scope.message = 
		{
			show 	      : false,
			resultType    : 'error',
			resultMessage : '',
		};

		$scope.error = {
			email			: false,
			password 	    : false,
			password_retype : false,
		};	
	};

	$scope.resetErrors();




	$scope.fields.login = 
	{
		email    : '',
		password : '',
	};



	$scope.retreiveToken = function(e)
	{
		e.preventDefault();

		$scope.resetErrors();

		// check if errors
		var validate = $rootScope.validateFields($scope.fields.login, $scope.login, $scope.error);
		if(validate === true)
		{
			// data
			var input = $scope.fields.login;

			// go register the user
			api.login(input.email, input.password);
		}
		else
		{
			$scope.message = validate;
		}
	};


	$scope.authenticateHandler = $rootScope.$on('authenticateLoaded', function(e, data)
	{
		console.log('Authenticated');
		var result = data;
		if(result != null)
		{
			$rootScope.user = result;
			console.log(result.name);
		}
	});


	$scope.loginHandler = $rootScope.$on('loginLoaded', function(e, data)
	{
		var result = data;

		// token
		if(result.api_token != null)
		{
			api.setApiToken(result.api_token);
			$rootScope.user = result;
		}

		// redirect to the main page
		$location.path('/load');
	});



	$scope.loginErrorHandler = $rootScope.$on('loginError', function(e, error)
	{
		$scope.message  = 
		{
			show          : true,
			resultType    : 'error',
			resultMessage : $rootScope.lang.no_valid_authentication,
		};

		$scope.error.email    = true;
		$scope.error.password = true;
	});



	$scope.fields.register = 
	{
		email			: '',
		password 		: '',		
		password_retype	: '',
	};


	
	$scope.registerUser = function(e)
	{
		// prevent default
		e.preventDefault();

		// reset the errors
		$scope.resetErrors();

		// set the errors
		var validate = $rootScope.validateFields($scope.fields.register, $scope.register, $scope.error);
		if(validate === true)
		{
			// go register the user
			var input = $scope.fields.register;

			api.registerUser(input.password, input.email);
		}
		else
		{
			$scope.message = validate;
		}
	};




	$scope.userRegisteredHandler = $rootScope.$on('registerLoaded', function(e, data)
	{
		var result = data;

		if(result.api_token != null)
		{
			api.setApiToken(result.api_token);
			$rootScope.user = result;
		}

		// set the status on registered
		$scope.formStatus = 'registered';
	});



	$scope.userRegisteredErrorHandler = $rootScope.$on('registerError', function(e, error)
	{
		// check email
		console.log(error);
		if(error.indexOf('email') !== -1)
			$scope.error.email = true;

		// check password
		if(error.indexOf('password') !== -1)
		{
			$scope.error.password 		 = true;
			$scope.error.password_retype = true;
		}

		// set the message
		$scope.message = 
		{
			show          : true,
			resultType    : 'error',
			resultMessage : $rootScope.lang[error],
		};
	});



	$scope.back = function()
	{
		$location.path('/login');
	};


	$scope.backListener = $rootScope.$on('backbutton', $scope.back);






	$scope.init();


	// remove the listeners
	$scope.$on('$destroy', function() 
    {
        $scope.removeListeners();
    });



    // remove listeners
    $scope.removeListeners = function()
    {
    	$scope.authenticateHandler();

    	$scope.loginHandler();
    	$scope.loginErrorHandler();

    	$scope.userRegisteredHandler();
    	$scope.userRegisteredErrorHandler();

    	$scope.backListener();
    };

});