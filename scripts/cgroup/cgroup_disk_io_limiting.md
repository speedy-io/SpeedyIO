# Limiting disk iops using cgroups

### Enabling cgroup v2 for I/O Throttling on NVMe (CentOS/RHEL 8/Stream)

Below is a concise, end-to-end guide that explains:
- Why cgroup v1 doesn’t work well for NVMe (multi-queue) devices.
- Why cgroup v2 doesn't work with centos 7, and hence you cannot use this approach for nvme drives on centos 7.
- How to disable cgroup v1 and enable cgroup v2 on a **CentOS 8/Stream 8** system.
- Verification steps at each stage to ensure everything is set up correctly.
- How to apply an IOPS limit (and confirm it’s working) with cgroup v2.

## Why cgroup v1 Does Not Work for NVMe Throttling
- **cgroup v1’s blkio controller** relies on the CFQ I/O scheduler, which is typically bypassed or unavailable on **NVMe** devices using modern multi-queue (MQ) schedulers (e.g., none, mq-deadline, bfq in MQ mode).
- As a result, **attempts to set blkio.throttle.\*** parameters on an NVMe partition/device in cgroup v1 **fail or do nothing**. You often see errors like `“the group can’t be modified”` or `“No such device.”`
- **cgroup v2** includes a **unified io controller** that supports throttling on multi-queue NVMe drives by interacting differently with the kernel’s block layer. This is why switching to cgroup v2 is essential for **I/O throttling** on modern devices.

## CentOS 7 does not support cgroup v2 (hence cannot do cgroups io throttling at all for nvme drives on centos 7)
- Officially, no. CentOS/RHEL 7 does not provide a fully functional cgroup v2 environment in its standard packages.
- Even if you run a newer custom kernel, systemd on CentOS 7 is still too old and lacks the necessary patches to manage cgroup v2.
- You therefore cannot reliably get a pure cgroup v2 hierarchy on CentOS 7.

## Steps to Disable cgroup v1 and Enable cgroup v2

**Note that v1 and v2 can co-exist in a hybrid mode, this guide aims to completely disable v1**

Use the enable-cgroupv2.sh script to disable cgroup v1 and enable v2.

## Applying an IOPS Limit on NVMe (cgroup v2)

5.1 Enable io Controller

By default, cgroup v2 controllers (like io) are disabled in the root. Enable it:

```
echo "+io" | sudo tee /sys/fs/cgroup/cgroup.subtree_control
```

(Also do +cpu or +memory if you want to manage those.)

5.2 Create a Child Cgroup

```
sudo mkdir /sys/fs/cgroup/slow_disk
```

5.3 Move Your Shell or Process Into This Cgroup

```
echo $$ | sudo tee /sys/fs/cgroup/slow_disk/cgroup.procs
```

Now any commands run from this shell are throttled.

5.4 Set I/O Limits

Identify the major:minor for your device (not the partition) via:

```
lsblk --output NAME,MAJ:MIN
```

For NVMe, you might see nvme0n1 259:0. Then:

```
echo "259:0 rbps=max wbps=max riops=100 wiops=50" | sudo tee /sys/fs/cgroup/slow_disk/io.max
```

This limits read IOPS to 100 and write IOPS to 50. Use `max` for no limit.

## Test the Throttling

1. Create a large file (say 10GB):

```
dd if=/dev/zero of=testfile bs=1M count=10240
```

2. Time the read (throttled):

```
time cat testfile > /dev/null
```

It should take noticeably longer compared to unthrottled.

3. Check counters:

```
cat /sys/fs/cgroup/slow_disk/io.stat
```

You’ll see `rios=` and `wios=` increment at a slower rate under throttling.

## Summary
- cgroup v1 cannot throttle I/O on NVMe due to the multi-queue scheduling.
- Enable cgroup v2 using the kernel params `systemd.unified_cgroup_hierarchy=1 cgroup_no_v1=all`, and rebuild GRUB with `sudo grub2-mkconfig -o /etc/grub.cfg` or `sudo /boot/efi/EFI/centos/grub.cfg`
- Verify with `cat /proc/cmdline` and `mount | grep cgroup`
- Use the cgroup v2 io controller by enabling `+io`, creating a sub-cgroup, assigning PIDs, and writing limits to io.max.
- Test with a large file read to see the actual throttling effect.

This setup ensures that NVMe devices are throttled correctly with cgroup v2, which was not possible with cgroup v1’s blkio subsystem.

---

## Optional steps

### Specify a particular kernel in grubby update
```
Specify a particular kernel (if you don't want to update all kernel commands), for example:
sudo grubby --update-kernel=/boot/vmlinuz-4.18.0-553.6.1.el8.x86_64 --args="systemd.unified_cgroup_hierarchy=1 cgroup_no_v1=all"
```
[specify-a-particular-kernel-in-grubby-update]: #specify-a-particular-kernel-in-grubby-update

### Legacy GRUB gotchas

* `grub2‑editenv list` may show variables that override your menu:
* `kernelopts=` – if present, *its* contents replace every `linux` line.
* `saved_entry=` – boots a specific stanza, ignoring the default.
* `tuned_params=` – injected by TuneD and expanded inside `$tuned_params`.
* Clear them if they contain conflicting options:

```bash
sudo grub2-editenv unset kernelopts saved_entry tuned_params
```

---

## Scripts for Throttling: test_throttling.sh and throttle_subscript.sh

You may want to automate the steps of creating a cgroup, setting io.max, running the I/O-heavy command, and then removing the cgroup. Two example scripts can be used:

- **throttle_subscript.sh**:  
  - Creates a named cgroup.  
  - Sets the I/O throttling parameters.  
  - Moves **itself** into that cgroup.  
  - Runs your specified command.  
  - Prints the cgroup name at the end (so you can remove it later).  

- **test_throttling.sh**:  
  - Calls `throttle_subscript.sh` with your chosen device major:minor and read/write IOPS (or `max` for no limit).
  - Use the **entire device's major:minor** and not the partition's major:minor (which gives some error). You can see the numbers by doing `lsblk`
  - Waits for that subscript to exit, leaving the cgroup empty.  
  - Removes (rmdir) the cgroup directory.  

### Why a separate subscript?
- If you put your **main shell** into the cgroup, you cannot remove it while the shell is still alive (“Device or resource busy”).  
- Some Bash sub-shells reuse the same PID as the parent, causing confusion. A separate script ensures a distinct PID.  
- Once that script exits, the cgroup is empty and removable.

### Example usage
In `test_throttling.sh`, you might do something like:

```bash
# test_throttling.sh
#!/usr/bin/env bash

echo "Running throttled I/O..."
CGROUP_NAME=$(
  ./throttle_subscript.sh "my_cg" "259:0" "max" "max" "100" "50" -- dd if=testfile of=/dev/null bs=1M
)

echo "Cgroup created: $CGROUP_NAME"
sudo rmdir "/sys/fs/cgroup/$CGROUP_NAME"
echo "Removed $CGROUP_NAME; done."
```

This ensures the subscript is the only process in that cgroup, and once it exits, you can safely remove it.
