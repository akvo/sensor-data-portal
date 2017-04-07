<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use App\Http\Controllers\Controller;
use App\User;
use App\Role;
use App\Sensor;
use DB;
use Hash;
use Image;
use Auth;

class UserController extends Controller
{

    /**
     * Display a listing of the resource.
     *
     * @return \Illuminate\Http\Response
     */
    public function index(Request $request)
    {
        $data = User::orderBy('id','DESC')->paginate(10);
            return view('users.index',compact('data'))
                ->with('i', ($request->input('page', 1) - 1) * 10);
    }

    /**
     * Show the form for creating a new resource.
     *
     * @return \Illuminate\Http\Response
     */
    public function create()
    {
        $roles = $this->getMyPermittedRoles(Auth::user());
        $sensors = Sensor::all()->pluck('name','id');
        return view('users.create',compact('roles','sensors'));
    }

    /**
     * Store a newly created resource in storage.
     *
     * @param  \Illuminate\Http\Request  $request
     * @return \Illuminate\Http\Response
     */
    public function store(Request $request)
    {
        if ($this->checkRoleAuthorization($request, "user-create") == false)
            return redirect()->route('users.index')->with('error', 'You are not allowed to create this type of user');

        $this->validate($request, [
            'name' => 'required',
            'email' => 'required|email|unique:users,email',
            'password' => 'required|same:confirm-password',
            'roles' => 'required'
        ]);

        $input = $request->all();
        $input['password'] = Hash::make($input['password']);
        $input['api_token'] = str_random(60);

        // Handle the user upload of avatar
        if($request->hasFile('avatar')){
            $avatar = $request->file('avatar');
            $filename = time() . '.' . $avatar->getClientOriginalExtension();
            Image::make($avatar)->resize(300, 300)->save( public_path('uploads/avatars/' . $filename ) );
            $user->avatar = $filename;
        }

        $user = User::create($input);
        
        // Handle role assignment, only store permitted role
        $roleIds = $this->getMyPermittedRoles(Auth::user(), true);
        foreach ($request->input('roles') as $key => $value)
        {
            if (in_array($value, $roleIds))
            {
                $user->attachRole($value);
            }
        }

        // Edit sensors
        if($request->has('sensors')){
            foreach ($request->input('sensors') as $key => $value) {
                DB::table('sensor_user')->insert(
                    ['user_id' => $user->id, 'sensor_id' => $value]
                );
            }
        }

        return redirect()->route('users.index')
                        ->with('success','User created successfully');
    }

    /**
     * Display the specified resource.
     *
     * @param  int  $id
     * @return \Illuminate\Http\Response
     */
    public function show($id)
    {
        $user = User::find($id);
        $sensors = DB::table('sensors')->join('sensor_user', 'sensors.id', '=', 'sensor_user.sensor_id')->where('user_id',$id)->orderBy('name','asc')->pluck('name','id');
        return view('users.show',compact('user','sensors'));
    }

    /**
     * Show the form for editing the specified resource.
     *
     * @param  int  $id
     * @return \Illuminate\Http\Response
     */
    public function edit($id)
    {
        $user       = User::find($id);
        $roles      = $this->getMyPermittedRoles($user);
        $userRole   = $user->roles->pluck('id','id')->toArray();
        $sensors    = DB::table('sensors')->orderBy('name','asc')->pluck('name','id');
        $userSensor = DB::table('sensor_user')->where('user_id',$id)->pluck('sensor_id','sensor_id')->toArray();

        return view('users.edit',compact('user','roles','userRole','sensors','userSensor'));
    }

    /**
     * Update the specified resource in storage.
     *
     * @param  \Illuminate\Http\Request  $request
     * @param  int  $id
     * @return \Illuminate\Http\Response
     */
    public function update(Request $request, $id)
    {
        if ($this->checkRoleAuthorization($request, "user-edit", $id) == false)
            return redirect()->route('users.index')->with('error', 'You are not allowed to edit this user');

        // Do normal validation
        $this->validate($request, [
            'name' => 'required',
            'email' => 'required|email|unique:users,email,'.$id,
            'password' => 'same:confirm-password',
            'roles' => 'required',
            'avatar' => 'mimes:jpeg,gif,png'
        ]);


        $input = $request->all();
        if(!empty($input['password'])){ 
            $input['password'] = Hash::make($input['password']);
        }else{
            $input = array_except($input,array('password'));    
        }

        $user = User::find($id);
        
        // Handle the user upload of avatar
        if($request->hasFile('avatar')){
            $avatar = $request->file('avatar');
            $filename = time() . '.' . $avatar->getClientOriginalExtension();
            Image::make($avatar)->fit(300, 300)->save( public_path('uploads/avatars/' . $filename ) );
            $user->avatar = $filename;
        }

        $user->update($input);

        // Edit role
        DB::table('role_user')->where('user_id',$id)->delete();
        foreach ($request->input('roles') as $key => $value) {
            $user->attachRole($value);
        }

        // Edit sensors
        if($request->has('sensors')){
            DB::table('sensor_user')->where('user_id',$id)->delete();
            foreach ($request->input('sensors') as $key => $value) {
                DB::table('sensor_user')->insert(
                    ['user_id' => $id, 'sensor_id' => $value]
                );
            }
        }

        return redirect()->route('users.index')
                        ->with("success", "User updated successfully");
    }

    /**
     * Remove the specified resource from storage.
     *
     * @param  int  $id
     * @return \Illuminate\Http\Response
     */
    public function destroy($id)
    {
        if ($this->checkRoleAuthorization(null, "user-delete", $id) == false)
            return redirect()->route('users.index')->with('error','User not deleted, you have no permission');


        User::find($id)->delete();
        return redirect()->route('users.index')
                        ->with('success','User deleted successfully');

    }

    private function checkRoleAuthorization($request=null, $permission=null, $id=null)
    {
        if ($id && Auth::user()->id == $id) // edit self is allowed
            return true;
     
        if ($permission && Auth::user()->can($permission) == false) // check permissions
            return false;

        // Check for unauthorized role editing
        if ($request)
        {
            $superId = Role::where('name','=','superadmin')->pluck('id','id')->toArray();
            $reqIsSup= count(array_diff($request->input('roles'), $superId)) == 0 ? true : false; // check if super admin id role is requested
            $roleIds = $this->getMyPermittedRoles(Auth::user(), true);
            $reqMatch= count(array_diff($request->input('roles'), $roleIds)) == 0 ? true : false; // check if all roles match

            if ($reqMatch == false || ($reqIsSup && Auth::user()->hasRole('superadmin') == false)){
                return false;
            }
        }
        return true;
    }

    // Helpers
    private function getMyPermittedRoles($user, $returnIdArray=false)
    {
        //die($user->roles->pluck('id'));
        if (Auth::user()->hasRole('superadmin'))
        {
            $roles = Role::all();
        }
        else if (Auth::user()->hasRole('admin'))
        {
            $roles = Role::where('name','!=','superadmin');
        }
        else 
        {
            $roles = $user->roles;
        }
        //die($roles);
        if ($returnIdArray)
        {
            return $roles->pluck('id','id')->toArray();
        } 
        else
        {
            return $roles->pluck('display_name','id');
        }
    }


}